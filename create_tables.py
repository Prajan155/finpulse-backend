from app.core.database import engine
from app.core.base import Base
from app.models.stock import Stock  # noqa: F401 
from app.models.price_history import PriceHistory  # noqa: F401

Base.metadata.create_all(bind=engine)
print("✅ Tables created.")
