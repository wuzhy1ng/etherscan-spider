from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from .tx import Tx

# 创建一个和mysql数据库之间的连接引擎对象
engine = create_engine("mysql://root:root@localhost/eth_tx_net", echo=True)

# 创建类
BaseModel = declarative_base()
Tx = Tx

# 创建一个连接会话对象；需要指定是和那个数据库引擎之间的会话
Session = sessionmaker(bind=engine)
session = Session()

BaseModel.metadata.create_all(engine)
