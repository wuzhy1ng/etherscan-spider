from sqlalchemy import Column, String, Integer

from . import BaseModel


class Tx(BaseModel):
    __tablename__ = 'tx'
    hash = Column(String(255), primary_key=True)
    block_number = Column(Integer)
    time_stamp = Column(Integer)
    fromAddress = Column(String(255))
    toAddress = Column(String(255))
    value = Column(String(255))
    gas = Column(String(255))
    gas_price = Column(String(255))
    gas_used = Column(String(255))
