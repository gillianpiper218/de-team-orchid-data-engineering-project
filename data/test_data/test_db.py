import json
from pprint import pprint

mock_table_name_list = (["address"], ["counterparty"], ["currency"], ["department"], ["design"], ["payment"], ["payment_type"], ["purchase_order"], ["sales_order"], ["staff"], ["transaction"])

with open('data/test_data/address.json', encoding='utf-8') as address_file:
    address_dict = json.load(address_file)

with open('data/test_data/counterparty.json', encoding='utf-8') as counterparty_file:
    counterparty_dict = json.load(counterparty_file)

with open('data/test_data/currency.json', encoding='utf-8') as currency_file:
    currency_dict = json.load(currency_file)

with open('data/test_data/department.json', encoding='utf-8') as department_file:
    department_dict = json.load(department_file)

with open('data/test_data/design.json', encoding='utf-8') as design_file:
    design_dict = json.load(design_file)

with open('data/test_data/payment.json', encoding='utf-8') as payment_file:
    payment_dict = json.load(payment_file)

with open('data/test_data/payment_type.json', encoding='utf-8') as payment_type_file:
    payment_type_dict = json.load(payment_type_file)

with open('data/test_data/purchase_order.json', encoding='utf-8') as purchase_order_file:
    purchase_order_dict = json.load(purchase_order_file)

with open('data/test_data/sales_order.json', encoding='utf-8') as sales_order_file:
    sales_order_dict = json.load(sales_order_file)

with open('data/test_data/staff.json', encoding='utf-8') as staff_file:
    staff_dict = json.load(staff_file)

with open('data/test_data/transaction.json', encoding='utf-8') as transaction_file:
    transaction_dict = json.load(transaction_file)

mock_db_data = []

mock_db_data.append(address_dict)
mock_db_data.append(counterparty_dict)
mock_db_data.append(currency_dict)
mock_db_data.append(department_dict)
mock_db_data.append(design_dict)
mock_db_data.append(payment_dict)
mock_db_data.append(payment_type_dict)
mock_db_data.append(purchase_order_dict)
mock_db_data.append(sales_order_dict)
mock_db_data.append(staff_dict)
mock_db_data.append(transaction_dict)

pprint(mock_db_data)
