from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import settings

# SQLAlchemy engine
mysql_engine = create_engine(settings.DATABASE_URL, echo=False)

# MySQL Session
MySQLSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=mysql_engine)

# Base class for MySQL ORM models
BaseMySQL = declarative_base()
