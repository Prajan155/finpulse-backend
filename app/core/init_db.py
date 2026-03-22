from app.core.base import Base
from app.core.database import engine
from app.models.stock import Stock
from app.models.user import User
from app.models.portfolio import PortfolioHolding

def init_db():
    Base.metadata.create_all(bind=engine)