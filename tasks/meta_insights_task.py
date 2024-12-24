from datetime import datetime
from pymongo import ReplaceOne, UpdateOne
from db.crud_mongodb import get_data, upsert_bulk_insights, update_data
from api_clients.meta_api import poll_meta_job_status, fetch_meta_insights
from utils.logging import setup_task_logger
from utils.batching import batch_processor
from schemas.meta_schema import MetricsList, MetaInsight, MetaInsightData, MetricsListLive, MetaInsightDataLive, MetaInsightLive
from concurrent.futures import ThreadPoolExecutor, as_completed

async def fetch_and_store_insights(level, scheduler_type: str = None, batch_size=10, max_workers=5):
    """
    Fetch and store Meta daily insights after polling job status.

    :param batch_size: Number of accounts to process in a batch.
    :param max_workers: Maximum number of concurrent threads for API calls.
    """
    logger = setup_task_logger("meta_daily_insights")

    # Step 1: Fetch pending report IDs
    pending_reports = list(get_data(
        "meta_references" if not scheduler_type else f"meta_references_{scheduler_type}", 
        {"level": level, "status": 0}
    ))

    if not pending_reports:
        logger.warning("No pending reports found for Meta daily insights.")
        return

    logger.info(f"Fetched {len(pending_reports)} pending reports.")

    def process_poll_job_status(report):
        """Poll job status synchronously."""
        try:
            job_ready = poll_meta_job_status(report['reference'], report['access_token'])
            return job_ready, report
        except Exception as e:
            logger.error(f"Error polling job status for account {report['account_id']}: {e}")
            return False, report

    def process_fetch_insights(report):
        """Fetch insights synchronously."""
        try:
            insights = fetch_meta_insights(report['reference'], report['access_token'])
            return insights, report
        except Exception as e:
            logger.error(f"Error fetching insights for account {report['account_id']}: {e}")
            return None, report

    async def process_batch(pending_reports):
        metrics_list = MetricsList().dict() if scheduler_type != "live" else MetricsListLive().dict()

        bulk_operations = []
        bulk_update_status = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Step 4: Poll job statuses concurrently
            futures_poll = {
                executor.submit(process_poll_job_status, report): report
                for report in pending_reports
            }

            completed_poll = []

            for future in as_completed(futures_poll):
                job_ready, report = future.result()
                if job_ready:
                    completed_poll.append(report)
                else:
                    logger.warning(f"Job not ready for account {report['account_id']} with reference id {report['reference']}. Skipping.")

            # Step 5: Fetch insights concurrently for ready jobs
            futures_fetch = {
                executor.submit(process_fetch_insights, report): report
                for report in completed_poll
            }

            for future in as_completed(futures_fetch):
                insights, report = future.result()

                bulk_update_status.append(
                  UpdateOne(
                    filter={"account_id": report["account_id"], "reference": report['reference']},
                    update={"$set": {"status": 1}}
                  )
                )

                if not insights:
                    logger.info(f"No insights found for account {report['account_id']}. Skipping.")
                    continue

                for insight in insights:
                  if 'account_id' not in insight:
                    insight['account_id'] = report.get("account_id")
                  elif "act_" not in insight.get("account_id"):
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

                logger.info(f"Fetched {len(insights)} insights for account {report['account_id']}.")

        # Step 7: Bulk upsert into MongoDB
        if bulk_operations:
            upsert_bulk_insights(f"meta_insights_{level}" if not scheduler_type else f"meta_insights_{level}_{scheduler_type}", bulk_operations)
            logger.info(f"Upserted {len(bulk_operations)} insights to MongoDB.")

        # Update the status in references collection
        if bulk_update_status:
            upsert_bulk_insights("meta_references" if not scheduler_type else f"meta_references_{scheduler_type}", bulk_update_status)
            logger.info(f"Updated status for {len(bulk_update_status)} reports.")

    # Step 8: Execute batch processing
    await batch_processor(pending_reports, process_batch, batch_size=batch_size)
    logger.info("Completed fetching and storing Meta insights.")
