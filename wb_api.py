import requests
from datetime import datetime, timedelta
from time import sleep
from math import ceil
import json
from dateutil import parser

# Connector for Wildberries API
class WBApiConn:
    def __init__(self, token):
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {self.token}',
        }

        # define endpoints
        self.content_base = 'https://content-api.wildberries.ru'
        self.prices_base = 'https://discounts-prices-api.wildberries.ru'
        self.statistics_base = 'https://statistics-api.wildberries.ru'
        self.analytics_base = 'https://seller-analytics-api.wildberries.ru'
        self.advert_base = 'https://advert-api.wildberries.ru'
        self.calendar_base = 'https://dp-calendar-api.wildberries.ru'

        # advert ID list predefined for functions
        self.adv_ids = []
        self.prom_ids = []

    # Get list of product cards (POST /content/v2/get/cards/list), period 30 minutes
    def get_product_cards(self) -> list:
        limit = 100
        url = f'{self.content_base}/content/v2/get/cards/list'
        post_data = {
          "settings": {
            "cursor": {
              "limit": limit
            },
            "filter": {
              "withPhoto": -1
            }
          }
        }

        result = []
        while True:
            raw_result = requests.post(url, headers=self.headers, json=post_data)
            # check status is 200 (OK)
            if raw_result.status_code != 200:
                print("Error on getting product cards\n"
                      f"Status code: {raw_result.status_code}\n"
                      f"Response: {raw_result.text}")
                return []
            sub_result = raw_result.json()

            result.extend(sub_result['cards'])
            if sub_result['cursor']['total'] < limit:
                break
            post_data['settings']['cursor']['nmID'] = sub_result['cursor']['nmID']
            post_data['settings']['cursor']['updatedAt'] = sub_result['cursor']['updatedAt']

        return result

    # Get list of product prices (GET /api/v2/list/goods/filter), period 30 minutes
    def get_product_prices(self) -> list:
        limit = 1000
        url = f'{self.prices_base}/api/v2/list/goods/filter'
        offset = 0

        result = []
        while True:
            raw_result = requests.get(url, headers=self.headers, params={'limit': limit, 'offset': offset})
            # check status is 200 (OK)
            if raw_result.status_code != 200:
                print("Error on getting product prices\n"
                      f"Status code: {raw_result.status_code}\n"
                      f"Response: {raw_result.text}")
                return []
            sub_result = raw_result.json()

            result.extend(sub_result['data']['listGoods'])
            if len(sub_result['data']['listGoods']) < limit:
                break
            offset += limit

        return result

    # Get last 30 minutes stats (GET /api/v1/supplier/{type}), period 30 minutes
    def get_stats(self, type, first_use=False) -> list: # type may be 'orders' or 'sales'
        # here's no pagination, so no limit and while loop
        url = f'{self.statistics_base}/api/v1/supplier/{type}'
        delta = timedelta(days=90) if first_use else timedelta(minutes=30) # need to get full stats on first DB fill
        time = datetime.now() - delta
        time_str = time.strftime('%Y-%m-%dT%H:%M:%S.%f')

        raw_result = requests.get(url, headers=self.headers, params={'dateFrom': time_str})
        # check status is 200 (OK)
        if raw_result.status_code != 200:
            print(f"Error on getting {type} stats\n"
                  f"Status code: {raw_result.status_code}\n"
                  f"Response: {raw_result.text}")
            return []

        result = raw_result.json()
        return result

    # Get report about products in warehouse (GET /api/v1/warehouse_remains), period 30 minutes
    def get_warehouses_report(self) -> list:
        base_url = f'{self.analytics_base}/api/v1/warehouse_remains'
        params = {'groupByBrand': 'true',
                  'groupBySubject': 'true',
                  'groupBySize': 'true',
                  'groupByNm': 'true',
                  'groupByBarcode': 'true',
                  'groupBySa': 'true'
        }

        # start report generation
        # NOTICE: fix for multi-threaded data receiver
        while True:
            report_id_raw = requests.get(base_url, params=params, headers=self.headers)
            if report_id_raw.status_code != 429:
                break
            sleep(60)

        # check status is 200 (OK)
        if report_id_raw.status_code != 200:
            print("Error on creating warehouse report\n"
                  f"Status code: {report_id_raw.status_code}\n"
                  f"Response: {report_id_raw.text}")
            return []

        report_id = report_id_raw.json()['data']['taskId']
        report_url = f'{base_url}/tasks/{report_id}'

        # wait for report generation
        while True:
            report_status = requests.get(f'{report_url}/status', headers=self.headers).json()['data']['status']
            if report_status == 'done':
                break
            sleep(5)

        # get report
        while True:
            report_raw = requests.get(f'{report_url}/download', headers=self.headers)
            if report_raw.status_code != 429:
                break
            sleep(60)

        # check status is 200 (OK)
        if report_raw.status_code != 200:
            print("Error on downloading warehouse report\n"
                  f"Status code: {report_raw.status_code}\n"
                  f"Response: {report_raw.text}")
            return []

        # a bit extend report with datetime
        result = report_raw.json()
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        for row in result:
            row['datetime'] = now

        return result

    # Get detailed financial reports (GET /api/v5/supplier/reportDetailByPeriod), period 24 hours
    def get_financial_report(self, first_use=False) -> list:
        limit = 100000
        url = f'{self.statistics_base}/api/v5/supplier/reportDetailByPeriod'

        endTime = datetime.now()
        startTime = endTime - timedelta(days=7)
        earliest = '2024-01-29' # date of creation this API method
        params = {'dateFrom': earliest if first_use else startTime.strftime('%Y-%m-%d'),
                  'dateTo': endTime.strftime('%Y-%m-%d'), 'rrdid': 0
        }

        result = []
        while True:
            raw_result = requests.get(url, headers=self.headers, params=params)
            # check status is 200 (OK)
            if raw_result.status_code != 200:
                print("Error on getting financial report\n"
                      f"Status code: {raw_result.status_code}\n"
                    f"Response: {raw_result.text}")
                return []

            sub_result = raw_result.json()
            result.extend(sub_result)
            if len(sub_result) < limit:
                break
            params['rrdid'] = sub_result[-1]['rrd_id']
            sleep(60) # to avoid 429 error

        return result

    # Get list of advertising campaigns (GET /adv/v1/promotion/count), period 30 minutes
    def get_adv_list(self):
        self.adv_ids.clear()
        url = f'{self.advert_base}/adv/v1/promotion/count'
        raw_result = requests.get(url, headers=self.headers)
        # check status is 200 (OK)
        if raw_result.status_code != 200:
            print("Error on getting advertising campaigns\n"
                  f"Status code: {raw_result.status_code}\n"
                  f"Response: {raw_result.text}")
            return

        result = raw_result.json()
        for adv_group in result['adverts']:
            for adv in adv_group['advert_list']:
                self.adv_ids.append(adv['advertId'])

    # Get details of advertising campaigns (POST /adv/v1/promotion/adverts), period 30 minutes
    def get_adv_deatils(self, first_use=False):
        limit = 50
        url = f'{self.advert_base}/adv/v1/promotion/adverts'

        self.get_adv_list()
        sleep(1) # to avoid 429 error
        blocks = ceil(len(self.adv_ids) / limit)

        result = []
        limit_dt = datetime.now() - timedelta(days=(30 if first_use else 0))

        type_decrypt = {
            4: "В каталоге",
            5: "В карточке товара",
            6: "В поиске",
            7: "На главной странице",
            8: "Авто-акция",
            9: "Аукцион"
        }
        status_decrypt = {
            -1: "Удаляется",
            4: "Ожидает запуска",
            7: "Завершена",
            8: "Отказ",
            9: "Идут показы",
            11: "Приостановлена"
        }
        payment_type_decrypt = {
            "cpm": "За показы",
            "cpo": "За заказы"
        }
        for i in range(blocks):
            start = i * limit
            end = min((i + 1) * limit, len(self.adv_ids))
            raw_result = requests.post(url, headers=self.headers, params={}, json=self.adv_ids[start:end])
            # check status is 200 (OK)
            if raw_result.status_code != 200:
                print("Error on getting advertising details\n"
                      f"Status code: {raw_result.status_code}\n"
                      f"Response: {raw_result.text}")
                return []

            sub_result = raw_result.json()
            for entry in sub_result:
                # unify params format
                if "autoParams" in entry:
                    entry["params"] = entry.pop("autoParams")
                if "unitedParams" in entry:
                    entry["params"] = entry.pop("unitedParams")

                # make some values human-readable
                entry['type'] = type_decrypt.get(entry['type'], 'Неизвестно')
                entry['status'] = status_decrypt.get(entry['status'], 'Неизвестно')
                entry['paymentType'] = payment_type_decrypt.get(entry['paymentType'], 'Неизвестно')

                # add advert to self.prom_ids if it's endTime >= today
                end_dt = parser.parse(entry['endTime'])
                if end_dt >= limit_dt.replace(tzinfo=end_dt.tzinfo): # to avoid comparing naive and aware datetimes
                    self.prom_ids.append(entry['advertId'])
                result.append(entry)
            sleep(0.5)

        return result

    # Get promotions statistics (POST /adv/v2/fullstats), period 24 hours
    def get_prom_stats(self, first_use=False):
        limit = 100
        url = f'{self.advert_base}/adv/v2/fullstats'

        if not self.prom_ids:
            print('WARNING: this method must be called after get_adv_deatils()!')
            return []
        blocks = ceil(len(self.prom_ids) / limit)

        result = []
        for i in range(blocks):
            start = i * limit
            end = min((i + 1) * limit, len(self.prom_ids))

            # build [{int}] from [int]
            # if first use, create interval with start = today - 30 days (limit of API), end = today
            post_body = []
            if first_use:
                interval = {'begin': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                            'end': datetime.now().strftime('%Y-%m-%d')}
                post_body.extend([{'id': adv_id, 'interval': interval} for adv_id in self.prom_ids[start:end]])
            else:
                post_body.extend([{'id': adv_id} for adv_id in self.prom_ids[start:end]])

            raw_result = requests.post(url, headers=self.headers, json=post_body)
            # check status is 200 (OK)
            if raw_result.status_code != 200:
                print("Error on getting promotions statistics\n"
                      f"Status code: {raw_result.status_code}\n"
                      f"Response: {raw_result.text}")
                sleep(60)
                continue

            json_result = raw_result.json()
            if not json_result:
                if i < blocks - 1: # don't wait if it's last iteration
                    sleep(60) # to avoid 429 error
                continue

            # linearize multi-level JSON
            for prom_stat in json_result:
                for day_stat in prom_stat['days']:
                    for app_stat in day_stat['apps']:
                        if not app_stat['nm']:
                            continue
                        for product_stat in app_stat['nm']:
                            product_stat['date'] = day_stat['date']
                            product_stat['advertId'] = prom_stat['advertId']
                            product_stat['appType'] = app_stat['appType']
                            result.append(product_stat)
            if i < blocks - 1: # don't wait if it's last iteration
                sleep(60) # to avoid 429 error

        return result

    # Get calendar of delivery points (GET /api/v1/calendar/XXX), period 30 minutes
    def get_promo_calendar(self):
        base_url = f'{self.calendar_base}/api/v1/calendar'

        # Get base list
        # datetime in format 'YYYY-MM-DDTHH:MM:SSZ' here
        # use 1970-01-01T00:00:00Z as start and now as end
        params = {'startDateTime': '1970-01-01T00:00:00Z',
                  'endDateTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                  'allPromo': False
        }
        base_list_raw = requests.get(f'{base_url}/promotions', headers=self.headers, params=params)
        # check status is 200 (OK)
        if base_list_raw.status_code != 200:
            print("Error on getting promotions list\n"
                  f"Status code: {base_list_raw.status_code}\n"
                  f"Response: {base_list_raw.text}")
            return []

        try:
            base_list = base_list_raw.json()['data']['promotions']
            id_list = [entry['id'] for entry in base_list]
        except KeyError:
            print("Something bad with response structure, maybe API is updated?")
            return []

        # Get detailed info
        details_limit = 100
        details_url = f'{base_url}/promotions/details'
        blocks = ceil(len(id_list) / details_limit)

        result = []
        for i in range(blocks):
            start = i * details_limit
            end = min((i + 1) * details_limit, len(id_list))

            details_params = {'promotionIDs': id_list[start:end]}
            raw_details = requests.get(details_url, headers=self.headers, params=details_params)
            # check status is 200 (OK)
            if raw_details.status_code != 200:
                print("Error on getting promotions details\n"
                      f"Status code: {raw_details.status_code}\n"
                      f"Response: {raw_details.text}")
                return []

            try:
                details = raw_details.json()['data']['promotions']
            except KeyError:
                print("Something bad with response structure, maybe API is updated?")
                return []

            for detailed_promo in details:
                if detailed_promo['type'] == 'regular':
                    # get list of products in promotion
                    list_url = f'{base_url}/promotions/nomenclatures'
                    list_params = {'promotionID': detailed_promo['id'],
                                   'inAction': True
                    }

                    nomenculatures_raw = requests.get(list_url, headers=self.headers, params=list_params)
                    # check status is 200 (OK)
                    if nomenculatures_raw.status_code != 200:
                        print(f'WARNING: error on getting nomenclatures for promotion {detailed_promo["id"]}')
                        detailed_promo['nomenclatures'] = []
                        result.append(detailed_promo)
                        sleep(0.7)
                        continue

                    try:
                        detailed_promo['nomenclatures'] = nomenculatures_raw.json()['data']['nomenclatures']
                    except KeyError:
                        print(f'WARNING: nomenclatures unavailable for promotion {detailed_promo["id"]}')
                        detailed_promo['nomenclatures'] = []
                    sleep(0.7)
                else:
                    detailed_promo['nomenclatures'] = []

                result.append(detailed_promo)
            sleep(0.7) # limit is 10 requests per 6 seconds

        return result


def main():
    token = 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjQxMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1MDQ4MDI0MSwiaWQiOiIwMTkzZTRlOC0yZGQ5LTc1MDgtYTk3My01ZjQzMTMyMzA2ZTEiLCJpaWQiOjEyNjg3ODA2LCJvaWQiOjc4ODE3LCJzIjoxMDczNzQ5NzU4LCJzaWQiOiJiNzJkNjdmYS1lY2MyLTU0NmYtODgyMi02MDJlYmM1YThkYTQiLCJ0IjpmYWxzZSwidWlkIjoxMjY4NzgwNn0.y0xfCzhKHgYwaWIU4BOllAfQvnVfRA78nPONHzjD9YwsAlZQnqy49tEja1M6N4rCK5IHROK7d5R8gO7_s8Qiog'
    conn = WBApiConn(token)
    res = [[] for _ in range(10)]
    #res[0] = conn.get_product_cards()
    #res[1] = conn.get_product_prices()
    #res[2] = conn.get_stats('orders')
    #res[3] = conn.get_stats('sales')
    #res[4] = conn.get_warehouses_report()
    #res[5] = conn.get_financial_report()
    #res[0] = conn.get_adv_deatils()
    #res[1] = conn.get_prom_stats()

    res[0] = conn.get_promo_calendar()
    json.dump(res, open('data.json', 'w'), indent=4, separators=(',', ': '), ensure_ascii=False)
    print('aaa')


if __name__ == '__main__':
    main()