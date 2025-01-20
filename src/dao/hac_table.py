from sqlmodel import Field, SQLModel
from typing import Optional

class hacTable(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    SEM_NUM_PAT: str
    SEM_HSEG_SOC: str
    SEM_FORMA_LEGAL: str
    SEM_NOMBRE: str
    SEM_COD_ACTIVOS: str
    SEM_SIC: str
    SEM_INDUSTRIA: str
    SEM_SIC_NEW: str
    SEM_INDUSTRIA_N: str
    NAICS_R02: str
    SEM_INDUS_R02: str
def create_hac(engine):
    SQLModel.metadata.create_all(engine)
