from sqlalchemy import Column, Integer, String, DateTime, Text, Enum
from db.base_mysql import BaseMySQL
from datetime import datetime

class UserAdAccountCredentialInformation(BaseMySQL):
    __tablename__ = "user_ad_account_credential_information"
    id = Column('id', Integer, primary_key=True, index=True)
    user_id = Column('user_id', Integer, nullable=False)
    account_type = Column('account_type', Integer, nullable=False)
    flag = Column('flag', Integer, default=1)
    token = Column('access_token', String(255))
    refresh_token = Column('refresh_token', String(255))
    created_at = Column('latest_binding', DateTime)
    updated_at = Column('latest_update_binding', DateTime, default=datetime.now())
    token_expiry = Column('token_expired_at', DateTime, default=datetime.now())

class UserAdAccountInformation(BaseMySQL):
    __tablename__ = "user_ad_account_information"
    id = Column('id', Integer, primary_key=True, index=True)
    account_id = Column('account_id', String(255), nullable=False)
    account_type = Column('account_type', Integer, nullable=False)
    latest_date_sync = Column('latest_data_synced', DateTime)
    latest_sync_time = Column('latest_updated_data_synced', DateTime)
    status = Column('status', Integer, default=1)

class AccountPlatform(BaseMySQL):
    __tablename__ = "account_platform"
    id = Column('id', Integer, primary_key=True, index=True)
    name = Column('name', String(255), nullable=False)

class AccountConfiguration(BaseMySQL):
    __tablename__ = "account_configuration"
    id = Column('id', Integer, primary_key=True, index=True)
    user_id = Column('user_id', Integer, nullable=False)
    account_type = Column('account_type', Integer, nullable=False)
    account_id = Column('account_platform_id', String(255))
    account_name = Column('account_platform_name', String(255))
    configuration = Column('account_platform_type', Text)
    currency = Column('currency', String(255))
    web_name = Column('web_name', String(255))
    web_link_address = Column('web_link_address', String(255))
    web_api_key = Column('web_api_key', String(255))
    web_api_password = Column('web_api_password', String(255))
    tag = Column('tag', String(255))

class User(BaseMySQL):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    password = Column(String(255), nullable=False)
    role = Column(Enum("admin", "user"), default="user")
    status = Column(Enum("active", "inactive"), default="inactive")
    registered_at = Column(DateTime, default=datetime.now())
    last_login = Column(DateTime, default=datetime.now())
    phone = Column(String(255), nullable=True)
    job_title = Column(String(255), nullable=True)
    company = Column(String(255), nullable=True)