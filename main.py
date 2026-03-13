from typing import Literal

from data_catalog_ingest import EntityManager, EntityManagerConfig
from data_catalog_ingest.exceptions import EntityNotFoundError
from data_catalog_ingest.models import (
    Artifact,
    ScientificWork,
)
from pydantic import AnyUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_LCLS_CATALOG = "slac-lcls-public-repository.slac-lcls-public-catalog"
MIME_PARQUET = "application/vnd.apache.parquet"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", env_file=".env")

    amsc_catalog_host: str = "localhost"
    amsc_catalog_port: int = 8585
    amsc_catalog_scheme: Literal["http", "https"] = "http"
    amsc_api_token: str
    amsc_catalog_fqn: str = DEFAULT_LCLS_CATALOG


settings = Settings()  # type: ignore[call-arg]

config = EntityManagerConfig.from_dict(
    {
        "backend_config": {
            "host": settings.amsc_catalog_host,
            "port": settings.amsc_catalog_port,
            "scheme": settings.amsc_catalog_scheme,
        }
    }
)
manager = EntityManager.from_config(config, token=settings.amsc_api_token)


def entity_fqn(entity: ScientificWork | Artifact) -> str:
    if isinstance(entity, ScientificWork):
        return f"{settings.amsc_catalog_fqn}.{entity.name}"

    return f"{entity.parent_fqn}.{entity.name}"


def upsert_entity(entity: ScientificWork | Artifact) -> str:
    fqn = entity_fqn(entity)

    try:
        manager.get(fqn)
        updated_fqn = manager.update(fqn, entity)
        print(f"  Updated: {updated_fqn}")
        return updated_fqn
    except EntityNotFoundError:
        created_fqn = manager.create(entity, catalog_fqn=settings.amsc_catalog_fqn)
        print(f"  Created: {created_fqn}")
        return created_fqn
    except Exception as e:
        print(f"  ERROR: {e}")
        raise


# 1. ScientificWork
print("Creating ScientificWork...")
sw = ScientificWork(
    name="nips3_multimodal_synthetic",
    display_name="NiPS3 Multimodal Synthetic Dataset",
    type="scientificWork",
    description=(
        "7,616 SpinW spin-wave theory simulations of NiPS3 (a van der Waals "
        "antiferromagnet) with randomized Hamiltonian parameters: exchange couplings "
        "J1a, J1b, J2a, J2b, J3a, J3b, J4 and single-ion anisotropy Ax, Az. Each "
        "simulation produced magnetization curves M(H) along a/b/c* axes (0-15 T) "
        "and inelastic neutron scattering spectra S(Q,E) (powder and high-symmetry). "
        "Summarized into three Parquet tables for analysis."
    ),
    location=AnyUrl("https://s3df.slac.stanford.edu/data/lcls/maiqmag/"),
)
sw_fqn = upsert_entity(sw)

# 2. Artifacts
artifacts = [
    # TODO: we can add raw hdf5 files later
    # this should really be an artifact collection when that feature is supported
    # Artifact(
    #     name="nips3_hdf5_data",
    #     display_name="NiPS3 Raw HDF5 Simulations",
    #     type="artifact",
    #     description=(
    #         "7,616 HDF5 files, one per SpinW simulation run. Each file contains "
    #         "scalar Hamiltonian parameters, magnetization curves M(H) along a/b/c* "
    #         "crystallographic axes, and INS spectra S(Q,E) for powder average and "
    #         "high-symmetry reciprocal-space paths."
    #     ),
    #     location=AnyUrl("https://drive.google.com/blah/"),
    #     parent_fqn=sw_fqn,
    #     format="application/x-hdf",
    # ),
    Artifact(
        name="nips3_parameters",
        display_name="NiPS3 Physics Parameters",
        type="artifact",
        description=(
            "7,616 rows x 11 cols. Dource of truth. Physics parameters "
            "extracted from HDF5 scalars: uid, key, J1a, J1b, J2a, J2b, J3a, J3b, "
            "J4 (exchange couplings, meV), Ax, Az (single-ion anisotropy, meV)."
        ),
        location=AnyUrl(
            "https://s3df.slac.stanford.edu/data/lcls/maiqmag/nips3_parameters.parquet"
        ),
        parent_fqn=sw_fqn,
        format=MIME_PARQUET,
    ),
    Artifact(
        name="nips3_mag_summary",
        display_name="NiPS3 Magnetization Summary",
        type="artifact",
        description=(
            "7,616 rows x 8 cols. Derived. Magnetization curve features: "
            "saturation magnetization (mag_a/b/cs_max), low-field susceptibility "
            "(mag_a/b/cs_slope_0), dominant axis."
        ),
        location=AnyUrl(
            "https://s3df.slac.stanford.edu/data/lcls/maiqmag/nips3_mag_summary.parquet"
        ),
        parent_fqn=sw_fqn,
        format=MIME_PARQUET,
    ),
    Artifact(
        name="nips3_ins_summary",
        display_name="NiPS3 INS Spectra Summary",
        type="artifact",
        description=(
            "7,616 rows x 6 cols. Derived. INS spectrum features: "
            "powder total weight, powder peak energy index, powder bandwidth, "
            "high-symmetry total weight, high-symmetry peak energy index."
        ),
        location=AnyUrl(
            "https://s3df.slac.stanford.edu/data/lcls/maiqmag/nips3_ins_summary.parquet"
        ),
        parent_fqn=sw_fqn,
        format=MIME_PARQUET,
    ),
]

print("Creating Artifacts...")
for art in artifacts:
    upsert_entity(art)

print("Done.")
