import local_data_handler
import data_validation_checks

def find_crate_type_distribution(orders_df):
    crate_distribution_df = orders_df.groupBy('company_id', 'crate_type').count().orderBy('company_id', 'crate_type')
    return crate_distribution_df

if __name__ == "__main__":
    # Outputs warning that company id and company name do not match 1 to 1 (casing and spelling issues)
    data_validation_checks.Test1_DataValidation_Checks().run_data_validation_checks()

    local_data = local_data_handler.LocalDataHandler()
    orders_df = local_data.orders_df
    crate_distribution_df = find_crate_type_distribution(orders_df)
    crate_distribution_df.show()

