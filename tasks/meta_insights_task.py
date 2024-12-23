from api_clients.meta_api import poll_meta_job_status, fetch_meta_insights
from utils.batching import batch_processor
from db.crud_mongodb import get_data, upsert_bulk_insights, update_data
from utils.logging import setup_task_logger
from pymongo import ReplaceOne
from schemas.meta_schema import MetricsList, MetaInsight, MetaInsightData, MetricsListLive, MetaInsightDataLive, MetaInsightLive
from datetime import datetime
import traceback
import json

async def fetch_and_store_insights(level, scheduler_type: str = None, batch_size: int = 10):
    """
    Fetch insights data and upsert it in MongoDB.
    :param batch_size: Number of report IDs to process in a batch.
    """
    
    logger = setup_task_logger("fetch_and_store_insights")

    # Step 1: Fetch pending report IDs
    pending_reports = list(get_data("meta_references" if not scheduler_type else f"meta_references_{scheduler_type}", {"level": level, "status": 0}))
    logger.info(f"Fetched {len(pending_reports)} pending reports.")

    # Step 2: Batch process report IDs
    async def process_batch(report_batch):
        """
        convert MetricsList to object
        """
        metrics_list = MetricsList().dict() if scheduler_type != "live" else MetricsListLive().dict()

        for report in report_batch:
            try:
                report_id = report["reference"]
                account_id = report["account_id"]
                token = report["access_token"]

                # Step 3: Check job status
                if poll_meta_job_status(report_id, token):
                    # Step 4: Fetch insights
                    insights_data = await fetch_meta_insights(report_id, token)

                    # Step 5: Prepare upsert operations
                    bulk_operations = []
                    for insight in insights_data:
                        if "act_" not in insight.get("account_id"):
                            insight["account_id"] = f"act_{insight.get('account_id')}"
                        
                        nested_metrics = {}
                        for field, value in insight.items():
                            if isinstance(value,list):
                                for val in value:
                                    action_type = field + "_" + val.get("action_type", "")
                                    if insight.get("attribution_setting") in ["multiple", "1d_view_7d_click", "1d_view_1d_click", "skan"]:
                                        if val.get("value"):
                                          if metrics_list.get(action_type):
                                              nested_metrics = {
                                                  **nested_metrics,
                                                  metrics_list.get(action_type): val.get("value", 0)
                                              }
                                    elif insight.get("attribution_setting") == "1d_view":
                                        if val.get("1d_view"):
                                          if metrics_list.get(action_type):
                                            nested_metrics = {
                                                **nested_metrics,
                                                metrics_list.get(action_type): val.get("1d_view", 0)
                                            }
                                    elif insight.get("attribution_setting") == "7d_click":
                                        if val.get("7d_click"):
                                          if metrics_list.get(action_type):
                                            nested_metrics = {
                                                **nested_metrics,
                                                metrics_list.get(action_type): val.get("7d_click", 0)
                                            }
                                    elif insight.get("attribution_setting") == "1d_click":
                                        if val.get("1d_click"):
                                          if metrics_list.get(action_type):
                                            nested_metrics = {
                                                **nested_metrics,
                                                metrics_list.get(action_type): val.get("1d_click", 0)
                                            }
                                    else:
                                        if val.get("value"):
                                          if metrics_list.get(action_type):
                                            nested_metrics = {
                                                **nested_metrics,
                                                metrics_list.get(action_type): val.get("value", 0)
                                            }
                                        
                        """
                        add data_metrics with nested_metrics
                        """
                        insight.update(nested_metrics)

                        data_metrics = MetaInsightData(**insight) if scheduler_type != "live" else MetaInsightDataLive(**insight)

                        """
                        check all value in data, if all value is 0 in replacement except in objective key, skip this record
                        """
                        if all(value == 0 for key, value in data_metrics.dict().items() if key != "objective"):
                            continue
                        
                        replacement = MetaInsight(
                          **insight, 
                          date=insight["date_start"], 
                          data=data_metrics
                          ).dict() if scheduler_type != "live" else MetaInsightLive(
                            **insight, 
                            date_end=insight["date_stop"], 
                            data=data_metrics
                          ).dict()

                        bulk_operations.append(
                          ReplaceOne(
                            filter={"account_id": insight["account_id"], "date": datetime.strptime(insight["date_start"], "%Y-%m-%d")},
                            replacement=replacement,
                            upsert=True
                          ) 
                        )

                    # Execute bulk upsert
                    if bulk_operations:
                        upsert_bulk_insights(f"meta_insights_{level}" if not scheduler_type else f"meta_insights_{level}_{scheduler_type}", bulk_operations)
                        logger.info(f"Upserted {len(bulk_operations)} insights for report_id {report_id}.")

                    # Update the status in references collection
                    # update_data("meta_references", {"account_data": account_id, "reference": report_id}, {"$set": {"status": 1}})
                    update_data("meta_references", {"account_data": account_id, "reference": report_id}, {"$set": {"status": 0}})
                    logger.info(f"Updated status for report_id {report_id}.")

            except Exception as e:
                traceback.print_exc()
                logger.error(f"Error processing report_id {report_id}: {e}")
                # failed_collection.insert_one({"report_id": report_id, "error": str(e)})

    await batch_processor(pending_reports, process_batch, batch_size=batch_size)
