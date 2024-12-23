from sqlalchemy.orm import Session
from db.models_mysql import UserAdAccountCredentialInformation, AccountPlatform, AccountConfiguration, User, UserAdAccountInformation
import sqlalchemy as sa

def get_access_tokens(db: Session, account_type: int):
    try: 
        return db.execute(
            sa.select(
                UserAdAccountCredentialInformation.id,
                UserAdAccountCredentialInformation.user_id,
                UserAdAccountCredentialInformation.account_type,
                UserAdAccountCredentialInformation.flag,
                UserAdAccountCredentialInformation.token,
                UserAdAccountCredentialInformation.refresh_token,
                UserAdAccountCredentialInformation.created_at,
                UserAdAccountCredentialInformation.updated_at,
                UserAdAccountCredentialInformation.token_expiry,
            ).where(UserAdAccountCredentialInformation.account_type == account_type)
        ).fetchall()
    except Exception as e:
        raise Exception(f"Error fetching Meta access tokens: {e}")
    
def get_account_ids_by_platform_id_and_user_id(db: Session, platform_id: int, user_id: int):
    try:
        return db.execute(
            sa.select(
                AccountConfiguration.account_id
            ).where(
                AccountConfiguration.account_type == platform_id,
                AccountConfiguration.user_id == user_id
            )
        ).fetchall()
    except Exception as e:
        raise Exception(f"Error fetching Meta account IDs: {e}")
    
def get_ad_account_platform_by_name(db: Session, name: str):
    try:
        return db.execute(
            sa.select(
                AccountPlatform.id,
                AccountPlatform.name
            ).where(AccountPlatform.name == name)
        ).fetchone()
    except Exception as e:
        raise Exception(f"Error fetching Meta platform: {e}")
    
def get_ad_account_platform_by_id(db: Session, platform_id: int):
    try:
        return db.execute(
            sa.select(
                AccountPlatform.id,
                AccountPlatform.name
            ).where(AccountPlatform.id == platform_id)
        ).fetchone()
    except Exception as e:
        raise Exception(f"Error fetching Meta platform: {e}")
    
def batch_update_user_credentials(db: Session, data: list):
    try:
        for record in data:
            update_values = {key: value for key, value in record.items() if key != "id"}
            db.execute(
                sa.update(UserAdAccountCredentialInformation)
                .where(UserAdAccountCredentialInformation.id == record["id"])
                .values(**update_values)
            )
        db.commit()
    except Exception as e:
        db.rollback()
    finally:
        db.close()

def get_all_access_tokens(db: Session):
    try:
        return db.execute(
            sa.select(
                UserAdAccountCredentialInformation.id,
                UserAdAccountCredentialInformation.user_id,
                UserAdAccountCredentialInformation.account_type,
                UserAdAccountCredentialInformation.flag,
                UserAdAccountCredentialInformation.token,
                UserAdAccountCredentialInformation.refresh_token,
                UserAdAccountCredentialInformation.created_at,
                UserAdAccountCredentialInformation.updated_at,
                UserAdAccountCredentialInformation.token_expiry,
                User.name,
                User.email
            ).join(User, User.id == UserAdAccountCredentialInformation.user_id)
        ).fetchall()
    except Exception as e:
        raise Exception(f"Error fetching all Meta access tokens: {e}")
    
def get_account_id_and_access_token_by_platform_id(db: Session, platform_id: int):
    try:
        return db.execute(
            sa.select(
                sa.distinct(UserAdAccountInformation.account_id).label('account_id'),  # Ensure unique account IDs
                UserAdAccountCredentialInformation.token,
                UserAdAccountCredentialInformation.refresh_token
            ).join(
                AccountConfiguration,
                UserAdAccountInformation.account_id == AccountConfiguration.account_id
            ).join(
                UserAdAccountCredentialInformation,
                sa.and_(
                    AccountConfiguration.account_type == UserAdAccountCredentialInformation.account_type,
                    UserAdAccountCredentialInformation.flag == 1  # Valid tokens only
                )
            ).where(
                AccountConfiguration.account_type == platform_id
            )
        ).fetchall()
    except Exception as e:
        raise Exception(f"Error fetching Meta account ID and access token: {e}")