from sqlalchemy.orm import Session
from db.models_mysql import UserAdAccountCredentialInformation, AccountPlatform, AccountConfiguration, User, UserAdAccountInformation
import sqlalchemy as sa
from sqlalchemy import distinct, func
from sqlalchemy.sql import and_

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
    """
    Fetch unique account IDs with access tokens and refresh tokens for the given platform ID.

    :param db: SQLAlchemy session.
    :param platform_id: Platform ID to filter accounts.
    :return: List of dictionaries containing account_id, access_token, and refresh_token.
    """
    try:
        # Step 1: Subquery to get active tokens (flag=1) for each user
        active_tokens_subquery = (
            sa.select(
                UserAdAccountCredentialInformation.user_id,
                UserAdAccountCredentialInformation.token.label("token"),
                UserAdAccountCredentialInformation.refresh_token.label("refresh_token"),
            )
            .where(UserAdAccountCredentialInformation.flag == 1)
            .distinct()
            .subquery()
        )

        # Step 2: Main query to join and filter by platform ID
        query = (
            sa.select(
                UserAdAccountInformation.account_id,
                active_tokens_subquery.c.token,
                active_tokens_subquery.c.refresh_token,
            )
            .distinct()
            .join(
                AccountConfiguration,
                UserAdAccountInformation.account_id == AccountConfiguration.account_id,
            )
            .join(
                active_tokens_subquery,
                AccountConfiguration.user_id == active_tokens_subquery.c.user_id,
            )
            .where(
                UserAdAccountInformation.account_type == platform_id
            )
        )

        # Step 3: Execute and return unique rows
        results = db.execute(query).fetchall()
        return list({row.account_id: row for row in results}.values())

    except Exception as e:
        raise Exception(f"Error fetching unique account ID and access token: {e}")
