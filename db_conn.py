from peewee import *
from playhouse.mysql_ext import JSONField
from wb_api import WBApiConn

# Establish a single database connection
mysql_db = MySQLDatabase(None)

class BaseModel(Model):
    class Meta:
        database = mysql_db

class ProductCards(BaseModel):
    auto_id = IntegerField(primary_key=True) # default auto_increment
    nmID = IntegerField(null=True)
    imtID = IntegerField(null=True)
    nmUUID = CharField(max_length=255, null=True)
    subjectID = IntegerField(null=True)
    subjectName = CharField(max_length=255, null=True)
    vendorCode = CharField(max_length=255, null=True)
    brand = CharField(max_length=255, null=True)
    title = CharField(max_length=255, null=True)
    description = TextField(null=True)
    needKiz = BooleanField(null=True)
    photos = JSONField(null=True)
    dimensions = JSONField(null=True)
    characteristics = JSONField(null=True)
    sizes = JSONField(null=True)
    tags = JSONField(null=True)
    createdAt = DateTimeField(null=True)
    updatedAt = DateTimeField(null=True)
    UserID = CharField(max_length=36, null=True)

    class Meta:
        table_name = 'ProductCards'
        indexes = (
            (('nmID', 'UserID'), True),
        )


class ProductPrices(BaseModel):
    auto_id = IntegerField(primary_key=True)  # default auto_increment
    nmID = IntegerField(null=True)
    vendorCode = CharField(max_length=255, null=True)
    sizes = JSONField(null=True)
    currencyIsoCode4217 = CharField(max_length=3, null=True)
    discount = IntegerField(null=True)
    clubDiscount = IntegerField(null=True)
    editableSizePrice = BooleanField(null=True)
    UserID = CharField(max_length=36, null=True)

    class Meta:
        table_name = "ProductPrices"
        indexes = (
            (('nmID', 'UserID'), True),
        )


class OrdersStats(BaseModel):
    auto_id = IntegerField(primary_key=True)  # default auto_increment
    date = DateTimeField(null=True)
    lastChangeDate = DateTimeField(null=True)
    warehouseName = CharField(max_length=255, null=True)
    warehouseType = CharField(max_length=255, null=True)
    countryName = CharField(max_length=255, null=True)
    oblastOkrugName = CharField(max_length=255, null=True)
    regionName = CharField(max_length=255, null=True)
    supplierArticle = CharField(max_length=255, null=True)
    nmId = IntegerField(null=True)
    barcode = CharField(max_length=255, null=True)
    category = CharField(max_length=255, null=True)
    subject = CharField(max_length=255, null=True)
    brand = CharField(max_length=255, null=True)
    techSize = CharField(max_length=255, null=True)
    incomeID = IntegerField(null=True)
    isSupply = BooleanField(null=True)
    isRealization = BooleanField(null=True)
    totalPrice = IntegerField(null=True)
    discountPercent = IntegerField(null=True)
    spp = IntegerField(null=True)
    finishedPrice = IntegerField(null=True)
    priceWithDisc = IntegerField(null=True)
    isCancel = BooleanField(null=True)
    cancelDate = DateTimeField(null=True)
    orderType = CharField(max_length=255, null=True)
    sticker = CharField(max_length=255, null=True)
    gNumber = CharField(max_length=255, null=True)
    srid = CharField(max_length=255, null=True)
    UserID = CharField(max_length=36, null=True)

    class Meta:
        table_name = "OrdersStats"
        indexes = (
            (('srid', 'UserID'), True),
        )


class SalesStats(BaseModel):
    auto_id = IntegerField(primary_key=True)  # default auto_increment
    date = DateTimeField(null=True)
    lastChangeDate = DateTimeField(null=True)
    warehouseName = CharField(max_length=255, null=True)
    warehouseType = CharField(max_length=255, null=True)
    countryName = CharField(max_length=255, null=True)
    oblastOkrugName = CharField(max_length=255, null=True)
    regionName = CharField(max_length=255, null=True)
    supplierArticle = CharField(max_length=255, null=True)
    nmId = IntegerField(null=True)
    barcode = CharField(max_length=255, null=True)
    category = CharField(max_length=255, null=True)
    subject = CharField(max_length=255, null=True)
    brand = CharField(max_length=255, null=True)
    techSize = CharField(max_length=255, null=True)
    incomeID = IntegerField(null=True)
    isSupply = BooleanField(null=True)
    isRealization = BooleanField(null=True)
    totalPrice = IntegerField(null=True)
    discountPercent = IntegerField(null=True)
    spp = IntegerField(null=True)
    paymentSaleAmount = IntegerField(null=True)
    forPay = FloatField(null=True)
    finishedPrice = IntegerField(null=True)
    priceWithDisc = IntegerField(null=True)
    saleID = CharField(max_length=255, null=True)
    orderType = CharField(max_length=255, null=True)
    sticker = CharField(max_length=255, null=True)
    gNumber = CharField(max_length=255, null=True)
    srid = CharField(max_length=255, null=True)
    UserID = CharField(max_length=36, null=True)

    class Meta:
        table_name = "SalesStats"
        indexes = (
            (('srid', 'UserID'), True),
        )


class WarehousesReport(BaseModel):
    auto_id = IntegerField(primary_key=True)  # default auto_increment
    brand = CharField(max_length=255, null=True)
    subjectName = CharField(max_length=255, null=True)
    nmId = IntegerField(null=True)
    barcode = CharField(max_length=255, null=True)
    vendorCode = CharField(max_length=255, null=True)
    techSize = CharField(max_length=255, null=True)
    volume = FloatField(null=True)
    inWayToClient = IntegerField(null=True)
    inWayFromClient = IntegerField(null=True)
    quantityWarehousesFull = IntegerField(null=True)
    warehouses = JSONField(null=True)
    datetime = DateTimeField(null=True)
    UserID = CharField(max_length=36, null=True)  # Always included

    class Meta:
        table_name = "WarehousesReport"
        indexes = (
            (('barcode', 'datetime', 'UserID'), True),
        )


class FinancialReport(BaseModel):
    auto_id = IntegerField(primary_key=True)  # default auto_increment
    realizationreport_id = IntegerField(null=True)
    date_from = DateField(null=True)
    date_to = DateField(null=True)
    create_dt = DateField(null=True)
    currency_name = CharField(max_length=255, null=True)
    suppliercontract_code = CharField(max_length=255, null=True)
    # for rrd_id we need to have 64 bit integer
    rrd_id = BigIntegerField(null=True)
    gi_id = IntegerField(null=True)
    fix_tariff_date_from = DateField(null=True)
    fix_tariff_date_to = DateField(null=True)
    subject_name = CharField(max_length=255, null=True)
    nm_id = IntegerField(null=True)
    brand_name = CharField(max_length=255, null=True)
    sa_name = CharField(max_length=255, null=True)
    ts_name = CharField(max_length=255, null=True)
    barcode = CharField(max_length=255, null=True)
    doc_type_name = CharField(max_length=255, null=True)
    quantity = IntegerField(null=True)
    retail_price = IntegerField(null=True)
    retail_amount = IntegerField(null=True)
    sale_percent = IntegerField(null=True)
    commission_percent = FloatField(null=True)
    office_name = CharField(max_length=255, null=True)
    supplier_oper_name = CharField(max_length=255, null=True)
    order_dt = DateTimeField(null=True)
    sale_dt = DateTimeField(null=True)
    rr_dt = DateTimeField(null=True)
    shk_id = BigIntegerField(null=True)
    retail_price_withdisc_rub = IntegerField(null=True)
    delivery_amount = IntegerField(null=True)
    return_amount = IntegerField(null=True)
    delivery_rub = IntegerField(null=True)
    gi_box_type_name = CharField(max_length=255, null=True)
    product_discount_for_report = IntegerField(null=True)
    supplier_promo = IntegerField(null=True)
    rid = IntegerField(null=True)
    ppvz_spp_prc = IntegerField(null=True)
    ppvz_kvw_prc_base = FloatField(null=True)
    ppvz_kvw_prc = FloatField(null=True)
    sup_rating_prc_up = IntegerField(null=True)
    is_kgvp_v2 = IntegerField(null=True)
    ppvz_sales_commission = IntegerField(null=True)
    ppvz_for_pay = FloatField(null=True)
    ppvz_reward = FloatField(null=True)
    acquiring_fee = FloatField(null=True)
    acquiring_percent = FloatField(null=True)
    acquiring_bank = CharField(max_length=255, null=True)
    ppvz_vw = FloatField(null=True)
    ppvz_vw_nds = FloatField(null=True)
    ppvz_office_id = IntegerField(null=True)
    ppvz_office_name = CharField(max_length=255, null=True)
    ppvz_supplier_id = IntegerField(null=True)
    ppvz_supplier_name = CharField(max_length=255, null=True)
    ppvz_inn = CharField(max_length=255, null=True)
    declaration_number = CharField(max_length=255, null=True)
    bonus_type_name = CharField(max_length=255, null=True)
    sticker_id = CharField(max_length=255, null=True)
    site_country = CharField(max_length=255, null=True)
    penalty = IntegerField(null=True)
    additional_payment = IntegerField(null=True)
    rebill_logistic_cost = IntegerField(null=True)
    rebill_logistic_org = CharField(max_length=255, null=True)
    kiz = CharField(max_length=255, null=True)
    storage_fee = IntegerField(null=True)
    deduction = IntegerField(null=True)
    acceptance = IntegerField(null=True)
    srid = CharField(max_length=255, null=True)
    report_type = IntegerField(null=True)
    assembly_id = IntegerField(null=True)
    is_srv_dbs = BooleanField(null=True)
    payment_processing = CharField(max_length=255, null=True)
    is_legal_entity = BooleanField(null=True)
    UserID = CharField(max_length=36, null=True)  # Always included

    class Meta:
        table_name = "FinancialReport"
        indexes = (
            (('rrd_id', 'UserID'), True),
        )


class ProductAdverts(BaseModel):
    auto_id = IntegerField(primary_key=True)  # default auto_increment
    endTime = DateTimeField(null=True)
    createTime = DateTimeField(null=True)
    changeTime = DateTimeField(null=True)
    startTime = DateTimeField(null=True)
    params = JSONField(null=True)
    name = CharField(max_length=255, null=True)
    dailyBudget = IntegerField(null=True)
    advertId = IntegerField(null=True)
    status = CharField(max_length=255, null=True)
    type = CharField(max_length=255, null=True)
    paymentType = CharField(max_length=255, null=True)
    searchPluseState = BooleanField(null=True)
    UserID = CharField(max_length=36, null=True)  # Always included

    class Meta:
        table_name = "ProductAdverts"
        indexes = (
            (('advertId', 'UserID'), True),
        )


class ProductPromos(BaseModel):
    auto_id = IntegerField(primary_key=True)  # default auto_increment
    views = IntegerField(null=True)
    clicks = IntegerField(null=True)
    ctr = FloatField(null=True)
    cpc = FloatField(null=True)
    sum = FloatField(null=True)
    atbs = IntegerField(null=True)
    orders = IntegerField(null=True)
    cr = FloatField(null=True)
    shks = IntegerField(null=True)
    sum_price = FloatField(null=True)
    name = CharField(max_length=255, null=True)
    nmId = IntegerField(null=True)
    date = DateTimeField(null=True)
    advertId = IntegerField(null=True)
    appType = IntegerField(null=True)
    UserID = CharField(max_length=36, null=True)  # Always included

    class Meta:
        table_name = "ProductPromos"
        # make combo (advertId, nmId, date, appType, userID) unique
        indexes = (
            (('advertId', 'nmId', 'date', 'appType', 'userID'), True),
        )


class PromoCalendar(BaseModel):
    auto_id = IntegerField(primary_key=True)  # default auto_increment
    id = IntegerField(null=True)
    name = CharField(max_length=255, null=True)
    description = TextField(null=True)
    advantages = JSONField(null=True)
    startDateTime = DateTimeField(null=True)
    endDateTime = DateTimeField(null=True)
    inPromoActionLeftovers = IntegerField(null=True)
    inPromoActionTotal = IntegerField(null=True)
    notInPromoActionLeftovers = IntegerField(null=True)
    notInPromoActionTotal = IntegerField(null=True)
    participationPercentage = IntegerField(null=True)
    type = CharField(max_length=255, null=True)
    exceptionProductsCount = IntegerField(null=True)
    ranging = JSONField(null=True)
    nomenclatures = JSONField(null=True)
    UserID = CharField(max_length=36, null=True)  # Always included

    class Meta:
        table_name = "PromoCalendar"
        indexes = (
            (('id', 'UserID'), True),
        )


class APIKeys(BaseModel):
    key_id = IntegerField(primary_key=True)
    user_id = CharField(max_length=36, null=True) # actually UUID
    api_key = CharField(max_length=512, null=True)
    runs = IntegerField(null=True, default=0)

    class Meta:
        table_name = "APIKeys"


class Writer:
    def __init__(self, token, user_id, run_number=0):
        self.conn = WBApiConn(token)
        self.user_id = user_id
        self.run_number = run_number
        # APIKeys should not be managed by Python, so it's not in tables_list
        self.tables_list = [ProductCards, ProductPrices, OrdersStats, SalesStats, WarehousesReport,
                            FinancialReport, ProductAdverts, ProductPromos, PromoCalendar]
        # mysql_db.connect() not needed, idk why but it's already connected

    def multi_insert(self, table, data_array):
        # By default, 'insert_many' method gives exception if in dict
        # there are keys that are not in table fields, so on every API update
        # we need to check if there are new fields in response and update code.
        # To avoid it, we'll create own method for inserting data, inserting
        # only fileds, presented in table.

        limit = 5000 # on more, especially for long rows about 1 KB, remote MySQL becomes buggy

        # remove all service fields
        fields = [field for field in list(table.__dict__.keys()) if field[0] != '_']
        fields.remove('DoesNotExist')

        # iterate array, for each element create dict with only fields
        result = []
        for data in data_array:
            data_dict = {key: data[key] for key in fields if key in data}
            # also automatically add APIKeyID :)
            data_dict['UserID'] = self.user_id
            result.append(data_dict)

        # we still should use 'insert_many' to avoid performance issues
        # due to our DB may be remote. also use atomic
        with mysql_db.atomic():
            for batch in chunked(result, limit):
                table.insert_many(batch).on_conflict_replace().execute()

    def init_tables(self):
        mysql_db.create_tables(self.tables_list)

    def update_data(self):
        print("Updating data...")

        # each 30 minutes
        print('Getting cards...')
        try:
            data = self.conn.get_product_cards()
            self.multi_insert(ProductCards, data)
        except Exception as e:
            print(f'Error while getting cards: {e}, skipping...')

        print('Getting prices...')
        try:
            data = self.conn.get_product_prices()
            self.multi_insert(ProductPrices, data)
        except Exception as e:
            print(f'Error while getting prices: {e}, skipping...')

        print('Getting orders stats...')
        # don't remove previous for dynamic stats
        try:
            data = self.conn.get_stats('orders', first_use=(self.run_number == 0))
            self.multi_insert(OrdersStats, data)
        except Exception as e:
            print(f'Error while getting orders stats: {e}, skipping...')

        print('Getting sales stats...')
        try:
            data = self.conn.get_stats('sales', first_use=(self.run_number == 0))
            self.multi_insert(SalesStats, data)
        except Exception as e:
            print(f'Error while getting sales stats: {e}, skipping...')

        print('Getting warehouses report...')
        try:
            data = self.conn.get_warehouses_report()
            self.multi_insert(WarehousesReport, data)
        except Exception as e:
            print(f'Error while getting warehouses report: {e}, skipping...')

        # run every day
        if self.run_number % (60 * 24 / 30) == 0:
            print('Getting financial report...')
            try:
                data = self.conn.get_financial_report(first_use=(self.run_number == 0))
                self.multi_insert(FinancialReport, data)
            except Exception as e:
                print(f'Error while getting financial report: {e}, skipping...')

        print('Getting product adverts...')
        try:
            data = self.conn.get_adv_deatils(first_use=(self.run_number == 0))
            self.multi_insert(ProductAdverts, data)
        except Exception as e:
            print(f'Error while getting product adverts: {e}, skipping...')

        # run every day
        if self.run_number % (60 * 24 / 30) == 0:
            print('Getting promos stats...')
            try:
                data = self.conn.get_prom_stats(first_use=(self.run_number == 0))
                self.multi_insert(ProductPromos, data)
            except Exception as e:
                print(f'Error while getting product promos: {e}, skipping...')

        print('Getting promo calendar...')
        try:
            data = self.conn.get_promo_calendar()
            self.multi_insert(PromoCalendar, data)
        except Exception as e:
            print(f'Error while getting promo calendar: {e}, skipping...')

        print('Done!')

    def delete_tables(self):
        mysql_db.drop_tables(self.tables_list)

    def run(self):

        self.init_tables()
        self.update_data()


if __name__ == '__main__':
    # Establish a single database connection
    mysql_db.init(
        'budibasemp',
        user='budibase',
        password='PASSWORD',
        host='147.45.168.55',
        port=3306
    )

    # connect to WB API
    token = 'TOKEN'
    user_id = 'USER_ID'
    writer = Writer(token, user_id)

    #writer.run()
    # drop tables
    writer.delete_tables()
