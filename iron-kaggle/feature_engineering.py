def dataset_ready(dataset):
    sales = pd.read_csv(dataset)
    sales.rename(columns={'Unnamed: 0':'index'},inplace=True)
    sales.set_index('index',inplace=True)
    sales.drop('date',axis=1,inplace=True)
    if 'sales' in sales.columns:
        sales_dummies = pd.get_dummies(sales, columns = ['state_holiday'])
        sales_dummies.drop(['open','state_holiday_a','state_holiday_b','state_holiday_c'],axis=1,inplace=True)
        sales_dummies = sales_dummies[sales_dummies['sales']!=0] 
        sales_train = sales_dummies[['day_of_week','nb_customers_on_day','promotion','school_holiday','sales','state_holiday_0']]
        return sales_train
    else:
        sales_dummies = pd.get_dummies(sales, columns = ['state_holiday'])
        sales_dummies.drop(['open','state_holiday_a','state_holiday_b','state_holiday_c'],axis=1,inplace=True)
        sales_train = sales_dummies[['day_of_week','nb_customers_on_day','promotion','school_holiday','state_holiday_0']]
        return sales_train