import time
from src.utils import load_config,get_spark_session,get_logger,resolve_path
from src.extract import extract_sales_data
from src.transform import clean_sales_data,add_derived_columns,aggregate_revenue_by_region_month
from src.load import write_table

def run_pipeline():
    config = load_config()
    logger = get_logger(config)
    spark = get_spark_session(config)

    start_time = time.time()
    logger.info ('=' * 60)
    logger.info(f'starting {config['app_name']}started at {start_time}')
    logger.info ('=' * 60)

    try:

        #extract
        input_path = resolve_path(config['paths']['raw_input'])
        logger.info(f'extracting data from {input_path} ')
        raw_df = extract_sales_data(spark, input_path)
        raw_count = raw_df.count()
        logger.info(f'extracted {raw_count} rows')

        # Transform
        logger.info('cleaning data now')
        cleaned_df = clean_sales_data(raw_df)
        clean_count = cleaned_df.count()
        logger.info(f'Cleaned data completed ,count {clean_count} remaining'
                f'dropped rows = {raw_df} - {cleaned_df}'  )
        logger.info ('adding derived columns')
        derived_df = add_derived_columns(cleaned_df)
        derived_count = derived_df.count()
        logger.info(f'derived columns added'
                    f'derived count is {derived_count}')
        derived_df.cache()
        logger.info('creating ')
        revenue_by_region_month_df = aggregate_revenue_by_region_month(derived_df)

        #load
        outdir_path = resolve_path(config['paths']['processed_output'])
        logger.info(f'writing to output path {outdir_path}')

        write_table(cleaned_df,outdir_path,'cleaned_sales')
        write_table(derived_df,outdir_path,'derived_sales')
        write_table(revenue_by_region_month_df,outdir_path,'revenue_by_region_month')

        elapsed = round(time.time() - start_time,2)
        logger.info(f'Pipeline is completed in {elapsed}s')
        logger.info('=' * 60)

    except Exception:
        logger.exception('pipline failed with unhandled exception')
        raise
    finally:
        spark.stop()

if __name__ == '__main__':
    run_pipeline()

        
        
    
