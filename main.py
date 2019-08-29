import urllib.parse
import json
from collections import namedtuple
import time
from datetime import datetime, timedelta

from shopify import Shopify


class ShopifyOrders(Shopify):

    def __init__(self, user, params, database='', table=''):
        super().__init__(user)
        self.api_call = '/admin/api/2019-10/orders.json'
        self.database = database
        self.table = table
        self.params = params
        self.json_list = []
        self.ids = ()
        self.parsed_list = []


    def get_data(self):
        pages_all = False

        while not pages_all:
            response = self.send_request(self.params)
            self.json_list.extend(response.json()['orders'])
            next_page = response.headers.get('Link', False)

            if not next_page:
                break

            #read link header
            link_string = next_page.split(',')
            #check if there are more then 1 pagination urls
            if len(link_string) > 1:
                pagination_string =  [pagination_url for pagination_url in link_string if 'rel="next"' in pagination_url][0].split(';')
            else:
                pagination_string = next_page.split(';')

            url= pagination_string[0].strip('<>')
            rel = pagination_string[1].replace(' rel=', '')

            if rel == '"previous"':
                break

            self.params = {
                k: v for k,v in urllib.parse.parse_qsl(url.split('?')[1])
            }

            time.sleep(1)

        if not self.json_list:
            print(f'No new in orders from {params["updated_at_min"]}')
            exit()

        return True


    def parse_result(self, json_list=''):

        if json_list == '': json_list = self.json_list

        for order in json_list:
            #fulfillemnts data
            if len(order['fulfillments']):
                fulfilled_at = order['fulfillments'][0]['updated_at']
            else:
                fulfilled_at = ''

            #discount
            if len(order['discount_codes']):
                discount_code = order['discount_codes'][0]['code']
            else:
                discount_code = ''

            #attributes
            if len(order['note_attributes']):
                note_attributes = ': '.join([
                    v for v in order['note_attributes'][0].values()
                ])
            else:
                note_attributes = ''

            #shipping info
            if order['shipping_lines']:
                shipping_price = order['shipping_lines'][0]['price']
                shipping_method = order['shipping_lines'][0]['code']
            else:
                shipping_price = ''
                shipping_method = ''

            #refund amount
            if len(order['refunds']):
                if len(order['refunds'][0]['transactions']):
                    refund_amount = order['refunds'][0]['transactions'][0]['amount']
                else:
                    refund_amount = ''
            else:
                refund_amount = ''

            #tracking info
            if order['fulfillments']:
                tracking_number = order['fulfillments'][0]['tracking_number']
                tracking_company = order['fulfillments'][0]['tracking_company']
            else:
                tracking_number = ''
                tracking_company = ''

            #correct weight calculation
            correct_total_weight = sum([float(item['grams']) for item in order['line_items']])

            #order lines
            for item in order['line_items']:
                #determine coefficient to get correct amount
                #for shipping, tax and discounts
                if correct_total_weight:
                    weight_coef = float(item['grams']) / (float(correct_total_weight))
                else:
                    weight_coef = 1

                #price coef
                sub_total_price = order['subtotal_price']
                if float(sub_total_price):
                    price_coef = float(item['price']) / float(sub_total_price)
                else:
                    price_coef = 0

                #calculate shipping price per item
                if shipping_price:
                    if weight_coef:
                        item_shipping_price = float(shipping_price) * weight_coef
                    else:
                        item_shipping_price = float(shipping_price) * price_coef
                else:
                    item_shipping_price = 0

                #taxes
                taxes_list = []
                item_total_tax = 0
                for el in item['tax_lines']:
                    taxes_list.append(el['title'] + ' ' + '{:.2%}'.format(float(el['rate'])))
                    taxes_list.append(el['price'])
                    item_total_tax += float(el['price'])

                taxes_list.extend([''] * (10 - len(taxes_list)))

                #discounts
                item_discount = 0
                if item['discount_allocations']:
                    for discount in item['discount_allocations']:
                        item_discount += float(discount['amount'])

                item_subtotal = item['quantity'] * float(item['price']) - item_discount
                item_total_price = item_subtotal + item_shipping_price + item_total_tax

                self.parsed_list.append((
                    order['created_at'],
                    str(order['name']),
                    str(order['email']),
                    str(order['financial_status']),
                    str(order['fulfillment_status']),
                    fulfilled_at,
                    str(order['customer']['accepts_marketing']),
                    str(order['currency']),
                    str(item_subtotal),
                    str(item_shipping_price),
                    str(item_total_tax),
                    str(item_total_price),
                    discount_code,
                    str(item_discount),
                    str(shipping_method),
                    str(item['quantity']),
                    str(item['name']),
                    str(item['price']),
                    str(item['sku']),
                    str(item['requires_shipping']),
                    str(item['taxable']),
                    str(item['fulfillment_status']),
                    str(order['billing_address']['name']),
                    str(order['billing_address']['address1']),
                    str(order['billing_address']['address2']),
                    str(order['billing_address']['company']),
                    str(order['billing_address']['city']),
                    str(order['billing_address']['zip']),
                    str(order['billing_address']['province']),
                    str(order['billing_address']['country']),
                    str(order['billing_address']['phone']),
                    str(order['shipping_address']['name']),
                    str(order['shipping_address']['address1']),
                    str(order['shipping_address']['address2']),
                    str(order['shipping_address']['company']),
                    str(order['shipping_address']['city']),
                    str(order['shipping_address']['zip']),
                    str(order['shipping_address']['province']),
                    str(order['shipping_address']['country']),
                    str(order['shipping_address']['phone']),
                    str(order['note']),
                    str(note_attributes),
                    str(order['cancelled_at']),
                    str(order['gateway']) + ' ' + str(order['processing_method']),
                    str(refund_amount),
                    order['name'].split('-')[0],
                    order['id'],
                    str(order['tags']),
                    str(order['source_name']),
                    *taxes_list,
                    str(tracking_number),
                    str(tracking_company)
                ))

        self.ids = tuple({order[46] for order in self.parsed_list})

        return True



class ShopifyTransactions(Shopify):

    def __init__(self, user, database='', table=''):
        super().__init__(user)
        self.database = database
        self.table = table
        self.order_id = ''
        self.json_list = []
        self.ids = ()
        self.parsed_list = []


    def get_data(self, orders_ids):
        count = 1
        for order_id in orders_ids:
            self.order_id = order_id
            self.api_call = f'/admin/api/2019-10/orders/{self.order_id}/transactions.json'
            response = self.send_request()
            self.json_list.extend(response.json()['transactions'])
            print(f'  {count} of {len(orders_ids)} has been added' , end='\r')
            count += 1
            time.sleep(0.3)

        return True


    def parse_result(self, json_list=''):
        if json_list == '': json_list = self.json_list

        for transaction in json_list:
            self.parsed_list.append((
                transaction['id'],
                transaction['order_id'],
                transaction['kind'],
                transaction['gateway'],
                transaction['created_at'],
                transaction['status'],
                transaction['receipt'].get('amount', transaction.get('amount', '')),
                transaction['receipt'].get('currency', transaction.get('currency', '')).lower(),
                transaction.get('payment_details', {}).get('credit_card_company', '')
            ))

        self.ids = tuple({transaction[0] for transaction in self.parsed_list})
        return True


def main():

    date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%S-04:00')
    orders_params = {
        'status': 'any',
        'limit': '250',
        'updated_at_min': date_from
    }

    shopify_orders = ShopifyOrders(user='user', params=orders_params, database='shopify', table='orders')
    shopify_transactions = ShopifyTransactions(user='user', database='shopify', table='transactions')

    shopify_orders.get_data()
    shopify_orders.parse_result()
    shopify_orders.mysql_delete_by_in(field='order_id', param=shopify_orders.ids)
    shopify_orders.mysql_add(data_to_add=shopify_orders.parsed_list)

    shopify_transactions.get_data(orders_ids=shopify_orders.ids)
    shopify_transactions.parse_result()
    shopify_transactions.mysql_delete_by_in(field='transaction_id', param=shopify_transactions.ids)
    shopify_transactions.mysql_add(data_to_add=shopify_transactions.parsed_list)


main()
