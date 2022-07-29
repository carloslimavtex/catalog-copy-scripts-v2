#!/usr/bin/python3

from xmlrpc.client import Boolean
from vtex_api import *
import configparser
import sqlite3
from sqlite3 import Error

## CONFIGURATION VARIABLES MOVED TO EXTERNAL FILE
## LOOK AT sample.ini

## Process Constants - TODO Move Category Array to Config File
BRANDS_API_PAGESIZE = 20000
CATEGORIES_API_LEVELS = 10
PRODUCT_QUANTITY = 50

categoryArray = [18,20,13,21,19,26,23,22,16,6,10,24,17,11,25,9,8]

config = configparser.ConfigParser()
config.read('crawl.ini')

script_verbose_mode = config['DEFAULT'].getboolean('script_verbose_mode')
vtex_from_account = config['DEFAULT']['vtex_from_account']
vtex_to_account = config['DEFAULT']['vtex_to_account']
vtex_from_account_name = config['DEFAULT']['vtex_from_account_name']
vtex_to_account_name = config['DEFAULT']['vtex_to_account_name']
vtex_from_account_VtexIdclientAutCookie = config['DEFAULT']['vtex_from_account_VtexIdclientAutCookie']
vtex_from_account_api_key = config['DEFAULT']['vtex_from_account_api_key']
vtex_from_account_api_token = config['DEFAULT']['vtex_from_account_api_token']
vtex_to_account_api_key = config['DEFAULT']['vtex_to_account_api_key']
vtex_to_account_api_token = config['DEFAULT']['vtex_to_account_api_token']
script_database_filename = vtex_from_account+'_TO_'+vtex_to_account+'.db'
franchise_accounts = (config['DEFAULT']['franchise_accounts'])
if len(franchise_accounts)>0: 
    franchise_accounts=franchise_accounts.split(',')
else:
    franchise_accounts=[vtex_to_account]
franchise_warehouse_id = config['DEFAULT']['franchise_warehouse_id']

skip_brands_step = config['STEPS'].getboolean('skip_brands_step')
skip_category_tree_step = config['STEPS'].getboolean('skip_category_tree_step')
skip_products_step = config['STEPS'].getboolean('skip_products_step')
skip_categories_step = config['STEPS'].getboolean('skip_categories_step')

delete_brands_from_account = config['DATABASE'].getboolean('delete_brands_from_account')
delete_brands_to_account = config['DATABASE'].getboolean('delete_brands_to_account')
delete_category_tree_from_account = config['DATABASE'].getboolean('delete_category_tree_from_account')
delete_category_tree_to_account = config['DATABASE'].getboolean('delete_category_tree_to_account')
delete_products_from_account = config['DATABASE'].getboolean('delete_products_from_account')
delete_products_to_account = config['DATABASE'].getboolean('delete_products_to_account')
delete_categories_from_account = config['DATABASE'].getboolean('delete_categories_from_account')
delete_categories_to_account = config['DATABASE'].getboolean('delete_categories_to_account')

if script_verbose_mode:
    print(f'vtex_from_account={vtex_from_account}')
    print(f'vtex_to_account={vtex_to_account}')
    print(f'vtex_from_account_VtexIdclientAutCookie={vtex_from_account_VtexIdclientAutCookie[0:10]}...{vtex_from_account_VtexIdclientAutCookie[-10:]}')
    print(f'vtex_from_account_api_key={vtex_from_account_api_key}')
    print(f'vtex_from_account_api_token={vtex_from_account_api_token[0:10]}...{vtex_from_account_api_token[-10:]}')
    print(f'vtex_to_account_api_key={vtex_to_account_api_key}')
    print(f'vtex_to_account_api_token={vtex_to_account_api_token[0:10]}...{vtex_to_account_api_token[-10:]}')
    print(f'script_database_filename={script_database_filename}')
    print(f'delete_brands_from_account={delete_brands_from_account}')
    print(f'delete_brands_to_account={delete_brands_to_account}')
    print(f'delete_categories_from_account={delete_category_tree_from_account}')
    print(f'delete_categories_to_account={delete_category_tree_to_account}')
    print(f'skip_brands_step={skip_brands_step}')
    print(f'skip_cateogories_step={skip_category_tree_step}')
    print(f'skip_products_step={skip_products_step}')
    print(f'skip_categories_step={skip_categories_step}')
    print('')

conn = sqlite3.connect(script_database_filename)

schemaScriptBrand = "CREATE TABLE Brand (account TEXT, id INTEGER, name TEXT, imageUrl TEXT, isActive INTEGER, title TEXT, metaTagDescription TEXT)"
schemaScriptCategoryTree = "CREATE TABLE CategoryTree (account TEXT, id INTEGER, name TEXT, hasChildren INTEGER, url TEXT, children TEXT, Title TEXT, MetaTagDescription TEXT)"
schemaScriptSpecification = "CREATE TABLE Specification (account TEXT, Id INTEGER, FieldTypeId INTEGER,CategoryId INTEGER, FieldGroupId INTEGER, Name TEXT, Description TEXT, Position INTEGER, IsFilter INTEGER, IsRequired INTEGER, IsOnProductDetails INTEGER, IsStockKeepingUnit INTEGER, IsWizard INTEGER, IsActive INTEGER, IsTopMenuLinkActive INTEGER, IsSideMenuLinkActive INTEGER, DefaultValue TEXT)"
schemaScriptSpecificationField = "CREATE TABLE SpecificationField (account TEXT, Name TEXT, CategoryId INTEGER, FieldId INTEGER, IsActive INTEGER, IsRequired INTEGER, FieldTypeId INTEGER, FieldValueId INTEGER, FieldTypeName TEXT, Description TEXT, IsStockKeepingUnit INTEGER, IsFilter INTEGER, IsOnProductDetails INTEGER, Position INTEGER, IsWizard INTEGER, IsTopMenuLinkActive INTEGER, IsSideMenuLinkActive INTEGER, DefaultValue TEXT, FieldGroupId INTEGER, FieldGroupName TEXT)"
schemaScriptSpecificationGroup = "CREATE TABLE SpecificationGroup (account TEXT, CategoryId INTEGER, Id INTEGER, Name TEXT, Position INTEGER)"
schemaScriptCategory = "CREATE TABLE Category (account TEXT, id INTEGER, name TEXT, FatherCategoryId INTEGER, Title TEXT, Description TEXT, Keywords TEXT, IsActive INTEGER, LomadeeCampaignCode TEXT, AdWordsRemarketingCode TEXT, ShowInStoreFront INTEGER, ShowBrandFilter INTEGER, ActiveStoreFrontLink INTEGER, GlobalCategoryId INTEGER, StockKeepingUnitSelectionMode TEXT, Score INTEGER, LinkId TEXT, HasChildren INTEGER)"
schemaScriptProduct = "CREATE TABLE Product (account TEXT, id INTEGER, name TEXT, departmentId INTEGER, categoryId INTEGER, brandId INTEGER, linkId TEXT, refId TEXT, isVisible INTEGER, description TEXT, descriptionShort TEXT, releaseDate TEXT, keywords TEXT, title TEXT, isActive INTEGER, taxCode TEXT, metaTagDescription TEXT, supplierId TEXT, showWithoutStock INTEGER, adWordsRemarketingCode TEXT, lomadeeCampaignCode TEXT, score INTEGER)"
schemaScriptSku = "CREATE TABLE Sku (account TEXT, id INTEGER, productId INTEGER, isActive INTEGER, name TEXT, refId INTEGER, PackagedHeight INTEGER, PackagedLength INTEGER, PackagedWidth INTEGER, PackagedWeightKg INTEGER, Height INTEGER, Length INTEGER, WeightKg INTEGER, Width INTEGER, CubicWeight INTEGER, IsKit INTEGER, CreationDate TEXT, RewardValue TEXT, EstimatedDateArrival TEXT, ManufacturerCode TEXT, CommercialConditionId INTEGER, MeasurementUnit TEXT, UnitMultiplier INTEGER, ModalType TEXT, KitItensSellApart INTEGER, Videos TEXT)"
schemaScriptImage = "CREATE TABLE Image (account TEXT, id INTEGER, ArchiveId INTEGER, SkuId INTEGER, Name TEXT, IsMain INTEGER, Label TEXT, Url TEXT)"
schemaScriptPrice = "CREATE TABLE Price (account TEXT, itemId TEXT, listPrice INTEGER, costPrice INTEGER, markup INTEGER, basePrice INTEGER, fixedPrices TEXT)"

cleanDataQueries = [f"UPDATE Product SET description=REPLACE(description,'{vtex_from_account_name}','{vtex_to_account_name}') WHERE description LIKE '%{vtex_from_account_name}%'",f"UPDATE Product SET descriptionShort=REPLACE(descriptionShort,'{vtex_from_account_name}','{vtex_to_account_name}') WHERE descriptionShort LIKE '%{vtex_from_account_name}%'",f"UPDATE Product SET metaTagDescription=REPLACE(metaTagDescription,'{vtex_from_account_name}','{vtex_to_account_name}') WHERE metaTagDescription LIKE '%{vtex_from_account_name}%'"]

def check_sqlite_schema(table_name,creation_script):
    """ check schema to determine if table exists
        if not, creates it
    :param table_name: name of table to check - example 'Brand'
    :param creation_script: create table statement - example 'CREATE TABLE Brand (brand_name TEXT)'
    :return: nothing
    """
    if table_name == "": return
    try:
        c = conn.cursor()
        c.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="{table_name}";')
        result = c.fetchone()
        if result == None:
            if script_verbose_mode:
                print(f'Table "{table_name}" Does Not Exist in "{script_database_filename}"') 
                print(f'Creating Table "{table_name}" Now in "{script_database_filename}"')
            c.execute(creation_script)
        else:
            if script_verbose_mode:
                print(f'Table "{table_name}" Exists in "{script_database_filename}"') 
    except Error as e:
        print(e)

def clean_table_before_inserts(vtex_account,table_name):
    """ delete all records associated with an account from table  
    :param vtex_account: name of account to use as DELETE filter - example 'ssesandbox04'
    :param table_name: name of the destination table for operation - example 'Product'
    :return: nothing
    """
    if table_name == "": return
    if vtex_account == "": return
    c = conn.cursor()
    c.execute(f'DELETE FROM {table_name} WHERE account="{vtex_account}"')
    conn.commit()

def find_missing_brands_for_products(vtex_from_account,vtex_to_account):
    """ lists all brands that need to be created on destination account based on the Product table
    :param vtex_from_account: name of source account
    :param vtex_to_account: name of destination account
    :return: returns list of tuples with Id and Name - example [(1,'First Brand'), (2,'Second Brand'), (3, 'Third Brand')]
    """
    if (vtex_from_account == "") or (vtex_to_account == ""): return
    c = conn.cursor()
    c.execute(f'SELECT id,name FROM Brand WHERE account="{vtex_from_account}" AND id IN (SELECT DISTINCT(brandid) FROM Product) AND id NOT IN (SELECT id FROM Brand WHERE account="{vtex_to_account}")')
    return c.fetchall()

def find_mismatching_brands_for_products(vtex_from_account,vtex_to_account):
    """ lists all brands that can't be created due to conflicts with destination account based on the Product table
    :param vtex_from_account: name of source account
    :param vtex_to_account: name of destination account
    :return: returns list of tuples with fromAccount, fromId, fromName, toAccount, toId, toName - example [('acc01',1,'First','acc02',1,'Not First')]
    """
    if (vtex_from_account == "") or (vtex_to_account == ""): return
    c = conn.cursor()
    c.execute(f'SELECT f.account AS fromAccount,f.id AS fromId,f.name AS fromName, t.account AS toAccount, t.id AS toId, t.name AS toName FROM Brand f INNER JOIN Brand t ON t.id=f.id WHERE t.account<>f.account AND fromName<>toName AND f.id IN (SELECT DISTINCT(brandid) FROM Product) AND fromAccount="{vtex_from_account}" ORDER BY fromAccount')
    return c.fetchall()

def find_missing_categories_for_products(vtex_from_account,vtex_to_account):
    """ lists all categories that need to be created on destination account based on the Product table
    :param vtex_from_account: name of source account
    :param vtex_to_account: name of destination account
    :return: returns list of tuples with Id and Name - example [(1,'First Category'), (2,'Second Category'), (3, 'Third Category')]
    """
    if (vtex_from_account == "") or (vtex_to_account == ""): return
    c = conn.cursor()
    c.execute(f'SELECT id,name FROM CategoryTree WHERE account="{vtex_from_account}" AND id IN (SELECT DISTINCT(categoryid) FROM Product) AND id NOT IN (SELECT id FROM CategoryTree WHERE account="{vtex_to_account}")')
    return c.fetchall()

def find_mismatching_categories_for_products(vtex_from_account,vtex_to_account):
    """ lists all categories that can't be created due to conflicts with destination account based on the Product table
    :param vtex_from_account: name of source account
    :param vtex_to_account: name of destination account
    :return: returns list of tuples with fromAccount, fromId, fromName, toAccount, toId, toName - example [('acc01',1,'First','acc02',1,'Not First')]
    """
    if (vtex_from_account == "") or (vtex_to_account == ""): return
    c = conn.cursor()
    c.execute(f'SELECT f.account AS fromAccount,f.id AS fromId,f.name AS fromName, t.account AS toAccount, t.id AS toId, t.name AS toName FROM CategoryTree f INNER JOIN CategoryTree t ON t.id=f.id WHERE t.account<>f.account AND fromName<>toName AND f.id IN (SELECT DISTINCT(categoryid) FROM Product) AND fromAccount="{vtex_from_account}" ORDER BY fromAccount')
    return c.fetchall()

def get_all_product_categories(vtex_from_account,vtex_to_account):
    """ retrieves all product categories
    :param vtex_from_account: name of source account
    :param vtex_to_account: name of destination account
    :return: returns list category ids - example [(1,),(2,),(3,)]
    """
    if (vtex_from_account == "") or (vtex_to_account == ""): return
    c = conn.cursor()
    c.execute(f'SELECT DISTINCT(categoryid) FROM Product WHERE account="{vtex_from_account}"')
    return c.fetchall()

def get_product_sku_ids(vtex_from_account,productid):
    """ retrieves all sku ids for a product 
    :param vtex_from_account: name of source account
    :param productid: id of product
    :return: returns list of sku ids - example [(1,),(2,),(3,)]
    """
    if (vtex_from_account == "") or (productid == ""): return
    c = conn.cursor()
    c.execute(f'SELECT id FROM Sku WHERE account="{vtex_from_account}" and productid={productid}')
    return c.fetchall()

def get_sku_images(vtex_account,skuid):
    """ retrieves all sku image data for a sku 
    :param vtex_account: name of account
    :param skuid: id of sku to get images
    :return: returns list of sku image info - example [(id1,archiveId1,SkuId1,Name1,isMain1,Label1,Url1,),(id2,archiveId2,SkuId2,Name2,isMain2,Label2,Url2,)]
    """
    if (vtex_account == "") or (skuid == ""): return
    c = conn.cursor()
    c.execute(f'SELECT id, ArchiveId, SkuId, Name, IsMain, Label, Url FROM Image WHERE account="{vtex_from_account}" and skuId={skuid}')
    return c.fetchall()

def get_sku_price_as_JSON(vtex_account,skuid):
    """ retrieves sku price as JSON (fixedPrices where removed because of error "invalid format for fixedPrices field"), 
    basePrices removed because of "The request is invalid: Item must have exactly two values filled between basePrice, costPrice and markup"
    :param vtex_account: name of account
    :param skuid: id of sku to get price
    :return: returns sku price info - example [(itemId, listPrice, costPrice, markup, basePrice, fixedPrices,)]
    """
    if (vtex_account == "") or (skuid == ""): return
    c = conn.cursor()
    c.execute(f'SELECT itemId, listPrice, costPrice, markup, basePrice, fixedPrices FROM Price WHERE account="{vtex_from_account}" and itemId={skuid} LIMIT 1')
    (itemId, listPrice, costPrice, markup, basePrice, fixedPrices) = c.fetchone()
    if markup:
        return({
            "itemId": itemId,
            "listPrice": listPrice,
            "costPrice": costPrice,
            "markup": markup,
            "fixedPrices": []
        })
    else:
        return({
            "itemId": itemId,
            "listPrice": listPrice,
            "costPrice": costPrice,
            "basePrice": basePrice,
            "fixedPrices": []
        })

def is_product_brand_ready_on_destination(vtex_from_account, vtex_to_account, brandid):
    """ check if the product brand is correct on destination account
    :param vtex_from_account: name of source account
    :param vtex_to_account: name of destination account
    :param brandid: id of the product brand
    :return: returns Tuple with (True,0,"Brand Ready on Destination") or (False,0,"Missing Brand on Destination") or (False,1,"Mismatched Brand on Destination")]
    """
    if (vtex_from_account == "") or (vtex_to_account == ""): return
    cur_from_account = conn.cursor()
    cur_from_account.execute(f'SELECT name FROM Brand WHERE account="{vtex_from_account}" and id={brandid}')
    res_from_account = cur_from_account.fetchall()
    cur_to_account = conn.cursor()
    cur_to_account.execute(f'SELECT name FROM Brand WHERE account="{vtex_to_account}" and id={brandid}')
    res_to_account = cur_to_account.fetchall()
    if (len(res_to_account)==0):
        return(False,0,"Missing Brand on Destination")
    else:
        (brandname_from,) = res_from_account[0]
        (brandname_to,) = res_to_account[0]
        if (brandname_to==brandname_from):
            return(True,0,"Brand Ready on Destination")
        else:
            return(False,1,f"Mismatched Brand: on Origin={brandname_from}({brandid}) on Destination={brandname_to}({brandid})")

def is_product_category_ready_on_destination(vtex_from_account, vtex_to_account, categoryid):
    """ check if the product category is correct on destination account
    :param vtex_from_account: name of source account
    :param vtex_to_account: name of destination account
    :param categoryid: id of the product category
    :return: returns Tuple with (True,0,"") or (False,0,"Missing Category") or (False,1,"Mismatched Brand")]
    """
    if (vtex_from_account == "") or (vtex_to_account == ""): return

    # TODO: check database vs API check logic here...
    ##### CHECK ON API!!!!
    goto_categories_cursor = conn.cursor()
    goto_json_source_category = read_api_as_JSON(vtex_api_to_account_cookies,vtex_api_to_account_headers,f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/category/{categoryid}')
    if goto_json_source_category: goto_categories_cursor.execute(convert_category_json_item_to_sql_insert(vtex_to_account, goto_json_source_category))
    conn.commit()

    cur_from_account = conn.cursor()
    cur_from_account.execute(f'SELECT name FROM Category WHERE account="{vtex_from_account}" and id={categoryid}')
    res_from_account = cur_from_account.fetchall()
    # print(f'SELECT name FROM Category WHERE account="{vtex_from_account}" and id={categoryid}')
    # print(f'res_from_account={res_from_account}')
    cur_to_account = conn.cursor()
    cur_to_account.execute(f'SELECT name FROM Category WHERE account="{vtex_to_account}" and id={categoryid}')
    res_to_account = cur_to_account.fetchall()
    if (len(res_to_account)==0):
        return(False,0,"Missing Category on Destination")
    else:
        (brandname_from,) = res_from_account[0]
        (brandname_to,) = res_to_account[0]
        if (brandname_to==brandname_from):
            return(True,0,"Category Ready on Destination")
        else:
            return(False,1,"Mismatched Category on Destination")

def get_all_product_data(vtex_account):
    """ retrieves all product data from table
    :param vtex_account: name of account to use as filter
    :return: returns list of product data tuples
    """
    if (vtex_account == ""): return
    c = conn.cursor()
    c.execute(f'SELECT id, name, departmentId, categoryId, brandId, linkId, refId, isVisible, description, descriptionShort, releaseDate, keywords, title, isActive, taxCode, metaTagDescription, supplierId, showWithoutStock, adWordsRemarketingCode, lomadeeCampaignCode, score FROM Product WHERE account="{vtex_account}"')
    return c.fetchall()

def get_brand_data_as_JSON(vtex_account,brandid):
    """ retrieves brand data from table
    :param vtex_account: name of account to use as filter
    :param brandid: id of brand to read
    :return: returns JSON with brand_data
    """
    if (vtex_account == "") or (brandid==None): return
    c = conn.cursor()
    c.execute(f'SELECT id, name, imageUrl, isActive, title, metaTagDescription FROM Brand WHERE account="{vtex_account}" AND id={brandid} LIMIT 1')
    (id, name, imageUrl, isActive, title, metaTagDescription) = c.fetchone()
    return({
        "id": id,
        "name": name,
        "imageUrl": imageUrl,
        "Active": isActive,
        "MenuHome": True,
        "Keywords": keywords,
        "title": title,
        "metaTagDescription": metaTagDescription
    })

def get_category_data_as_JSON(vtex_account,categoryid):
    """ retrieves category data from table
    :param vtex_account: name of account to use as filter
    :param categoryid: id of category to read
    :return: returns JSON with category_data
    """
    # TODO: fix ShowBrandFilter issues (None => false), isActive, ShowInStoreFront, ActiveStoreFrontLink
    if (vtex_account == "") or (categoryid==None): return
    c = conn.cursor()
    c.execute(f'SELECT id, name, FatherCategoryId, Title, Description, Keywords, IsActive, LomadeeCampaignCode, AdWordsRemarketingCode, ShowInStoreFront, ShowBrandFilter, ActiveStoreFrontLink, GlobalCategoryId, StockKeepingUnitSelectionMode, Score, LinkId, HasChildren FROM Category WHERE account="{vtex_account}" AND id={categoryid} LIMIT 1')
    (id, name, FatherCategoryId, Title, Description, Keywords, IsActive, LomadeeCampaignCode, AdWordsRemarketingCode, ShowInStoreFront, ShowBrandFilter, ActiveStoreFrontLink, GlobalCategoryId, StockKeepingUnitSelectionMode, Score, LinkId, HasChildren) = c.fetchone()
    return({
        "Id": id,
        "Name": name,
        "Keywords": Keywords,
        "Title": Title,
        "Description": Description,
        "FatherCategoryId": FatherCategoryId,
        "GlobalCategoryId": GlobalCategoryId,
        "ShowInStoreFront": True,
        "IsActive": True,
        "ActiveStoreFrontLink": True,
        "ShowBrandFilter": True,
        "Score": Score,
        "StockKeepingUnitSelectionMode": StockKeepingUnitSelectionMode,
        "LomadeeCampaignCode": LomadeeCampaignCode,
        "AdWordsRemarketingCode": AdWordsRemarketingCode
    })

def get_sku_data_as_JSON(vtex_account,skuid):
    """ retrieves sku data from table
    :param vtex_account: name of account to use as filter
    :param skuid: id of sku to read
    :return: returns JSON with sku_data
    """
    if (vtex_account == "") or (skuid==None): return
    c = conn.cursor()
    c.execute(f'SELECT id, productId, isActive, name, refId, PackagedHeight, PackagedLength, PackagedWidth, PackagedWeightKg, Height, Length, WeightKg, Width, CubicWeight, IsKit, CreationDate, RewardValue, EstimatedDateArrival, ManufacturerCode, CommercialConditionId, MeasurementUnit, UnitMultiplier, ModalType, KitItensSellApart, Videos FROM Sku WHERE account="{vtex_account}" AND id={skuid}')
    (id, productId, isActive, name, refId, PackagedHeight, PackagedLength, PackagedWidth, PackagedWeightKg, Height, Length, WeightKg, Width, CubicWeight, IsKit, CreationDate, RewardValue, EstimatedDateArrival, ManufacturerCode, CommercialConditionId, MeasurementUnit, UnitMultiplier, ModalType, KitItensSellApart, Videos) = c.fetchone()
    return({
        "Id": id,
        "ProductId": productId,
        "IsActive": False,
        "Name": name,
        "RefId": refId,
        "PackagedHeight": PackagedHeight,
        "PackagedLength": PackagedLength,
        "PackagedWidth": PackagedWidth,
        "PackagedWeightKg": PackagedWeightKg,
        "Height": Height,
        "Length": Length,
        "Width": Width,
        "WeightKg": WeightKg,
        "CubicWeight": CubicWeight,
        "IsKit": IsKit,
        "CreationDate": CreationDate,
        "RewardValue": RewardValue,
        "EstimatedDateArrival": EstimatedDateArrival,
        "ManufacturerCode": ManufacturerCode,
        "CommercialConditionId": CommercialConditionId,
        "MeasurementUnit": MeasurementUnit,
        "UnitMultiplier": UnitMultiplier,
        "ModalType": ModalType,
        "KitItensSellApart": KitItensSellApart,
        "Videos": Videos,
        "ActivateIfPossible": True
    })

def get_sample_sku_stock_JSON():
    return({
        "unlimitedQuantity": False,
        "quantity": 999
    })

def validate_token_on_account(validation_cookies, validation_headers, vtex_account,account_type):
    """ calls VTEX account API to check if token is valid
    :param vtex_account: name of account to call API - example 'ssesandbox04'
    :param account_type: description of type of account - could be "SOURCE" or "DESTINATION"
    :return: returns True if token is valid otherwise returns False
    """
    if len(vtex_account)<1: return False
    sample_data_vtex_account = read_api_as_JSON(validation_cookies,validation_headers,f'https://{vtex_account}.vtexcommercestable.com.br/api/catalog_system/pvt/brand/pagedlist?pageSize={str(BRANDS_API_PAGESIZE)}&page=1')
    if sample_data_vtex_account:
        if script_verbose_mode: print(f'[INFO] ACCESS VALIDATED ON {account_type} ACCOUNT: "{vtex_account}"')
        return True
    else:
        print(f'[ERROR] INVALID ACCESS (VtexIdclientAutCookie or AppKey+AppToken) ON {account_type} ACCOUNT OR INVALID {account_type} ACCOUNT: "{vtex_account}", PLEASE CHECK.')
        return False

def convert_brand_json_item_to_sql_insert(vtex_account, brand_item_json):
    """ transforms a JSON object from VTEX Brand API into a valid SQL INSERT STATEMENT.
        variables with value None will be converted into empty strings("")
    :param vtex_account: name of the account providing Brand data - example 'ssesandbox04'
    :param brand_item_json: json payload as retrieved from API https://developers.vtex.com/vtex-rest-api/reference/catalog-api-brand#brandlistperpage
    :return: string with SQL statement
    """
    if (brand_item_json['name']) and (brand_item_json['name'].count('"')>0):
        brand_item_json['name'] = brand_item_json['name'].replace('"','')
    if (brand_item_json['metaTagDescription']) and (brand_item_json['metaTagDescription'].count('"')>0):
        brand_item_json['metaTagDescription'] = brand_item_json['MetaTagDescription'].replace('"','')
    
    return(f'INSERT INTO Brand (account,id,name,imageUrl,isActive,title,metaTagDescription) VALUES ("{vtex_account}",{brand_item_json["id"]},"{str(brand_item_json["name"] or "")}","{str(brand_item_json["imageUrl"] or "")}",{brand_item_json["isActive"]},"{str(brand_item_json["title"] or "")}","{str(brand_item_json["metaTagDescription"] or "")}")')

def convert_category_tree_json_item_to_sql_insert(vtex_account, category_tree_item_json):
    """ transforms a JSON object from VTEX Category Tree API into 
        a valid SQL INSERT STATEMENT.
        variables with value None will be converted into empty strings("")
    :param vtex_account: name of the account providing Category data - example 'ssesandbox04'
    :param category_tree_item_json: json payload as retreived from API https://developers.vtex.com/vtex-rest-api/reference/catalog-api-category#catalog-api-get-category-tree
    :return: string with SQL statement
    """
    if (category_tree_item_json['name']) and (category_tree_item_json['name'].count('"')>0):
        category_tree_item_json['name'] = category_tree_item_json['name'].replace('"','')
    if (category_tree_item_json['MetaTagDescription']) and (category_tree_item_json['MetaTagDescription'].count('"')>0):
        category_tree_item_json['MetaTagDescription'] = category_tree_item_json['MetaTagDescription'].replace('"','')
    
    return(f'INSERT INTO CategoryTree (account,id,name,hasChildren,url,children,Title,metaTagDescription) VALUES ("{vtex_account}",{category_tree_item_json["id"]},"{str(category_tree_item_json["name"] or "")}",{category_tree_item_json["hasChildren"]},"{str(category_tree_item_json["url"] or "")}","{str(category_tree_item_json["children"]) and ""}","{str(category_tree_item_json["Title"] or "")}","{str(category_tree_item_json["MetaTagDescription"] or "")}")')

def convert_category_json_item_to_sql_insert(vtex_account, category_item_json):
    """ transforms a JSON object from VTEX Category  API into a valid SQL INSERT STATEMENT.
        variables with value None will be converted into empty strings("")
    :param vtex_account: name of the account providing Category data - example 'ssesandbox04'
    :param category_tree_item_json: json payload as retreived from API https://developers.vtex.com/vtex-rest-api/reference/catalog-api-get-category
    :return: string with SQL statement
    """
    if (category_item_json['Name']) and (category_item_json['Name'].count('"')>0):
        category_item_json['Name'] = category_item_json['Name'].replace('"','')
    if (category_item_json['Description']) and (category_item_json['Description'].count('"')>0):
        category_item_json['Description'] = category_item_json['Description'].replace('"','')
    
    return(f'INSERT INTO Category (account,id,name,FatherCategoryId,Title,Description,Keywords,IsActive,LomadeeCampaignCode,AdWordsRemarketingCode,ShowInStoreFront,ShowBrandFilter,ActiveStoreFrontLink,GlobalCategoryId,StockKeepingUnitSelectionMode,Score,LinkId,HasChildren) VALUES ("{vtex_account}",{category_item_json["Id"]},"{str(category_item_json["Name"] or "")}",{str(category_item_json["FatherCategoryId"] or "null")},"{str(category_item_json["Title"] or "")}","{str(category_item_json["Description"]) or ""}","{str(category_item_json["Keywords"] or "")}",{str(category_item_json["IsActive"] or "null")},"{str(category_item_json["LomadeeCampaignCode"] or "")}","{str(category_item_json["AdWordsRemarketingCode"] or "")}",{str(category_item_json["ShowInStoreFront"] or "null")},{str(category_item_json["ShowBrandFilter"] or "null")},{str(category_item_json["ActiveStoreFrontLink"] or "null")},"{str(category_item_json["GlobalCategoryId"] or "")}","{str(category_item_json["StockKeepingUnitSelectionMode"] or "")}",{str(category_item_json["Score"] or "null")},"{str(category_item_json["LinkId"] or "")}",{str(category_item_json["HasChildren"] or "null")})')

def convert_specification_group_json_item_to_sql_insert(vtex_account, specification_group_json):
    """ transforms a JSON object from VTEX Specifications Group API into a valid SQL INSERT STATEMENT.
        variables with value None will be converted into empty strings("")
    :param vtex_account: name of the account providing Specifications Group data - example 'ssesandbox04'
    :param specification_json: json payload as retreived from API https://developers.vtex.com/vtex-rest-api/reference/catalog-api-get-specification-group
    :return: string with SQL statement
    """

    return(f'INSERT INTO SpecificationGroup (account, CategoryId, Id, Name, Position) VALUES ("{vtex_account}",{str(specification_group_json["CategoryId"] or "")},{specification_group_json["Id"] or ""},"{str(specification_group_json["Name"] or "")}",{str(specification_group_json["Position"] or "")})')

def convert_specification_json_item_to_sql_insert(vtex_account, specification_json):
    """ transforms a JSON object from VTEX Specification API into a valid SQL INSERT STATEMENT.
        variables with value None will be converted into empty strings("")
    :param vtex_account: name of the account providing Specification data - example 'ssesandbox04'
    :param specification_json: json payload as retreived from API https://developers.vtex.com/vtex-rest-api/reference/get_api-catalog-pvt-specification-specificationid
    :return: string with SQL statement
    """

    return(f'INSERT INTO Specification (account, Id, FieldTypeId, CategoryId, FieldGroupId, Name, Description, Position, IsFilter, IsRequired, IsOnProductDetails, IsStockKeepingUnit, IsWizard, IsActive, IsTopMenuLinkActive, IsSideMenuLinkActive, DefaultValue) VALUES ("{vtex_account}",{str(specification_json["Id"] or "")},{specification_json["FieldTypeId"] or ""},{str(specification_json["CategoryId"] or "")},{str(specification_json["FieldGroupId"] or "")},"{str(specification_json["Name"] or "")}","{str(specification_json["Description"]) or ""}",{str(specification_json["Position"] or "")},{str(specification_json["IsFilter"] or "")},{str(specification_json["IsRequired"] or "")},{str(specification_json["IsOnProductDetails"] or "True")},{str(specification_json["IsStockKeepingUnit"] or "False")},{str(specification_json["IsWizard"] or "False")},{str(specification_json["IsActive"] or "True")},{str(specification_json["IsTopMenuLinkActive"] or "True")},{str(specification_json["IsSideMenuLinkActive"] or "True")},"{str(specification_json["DefaultValue"] or "")}")')

def convert_specification_field_json_to_sql_insert(vtex_account, specification_field_json):
    """ transforms a JSON object from VTEX Specification Field API into a valid SQL INSERT STATEMENT.
        variables with value None will be converted into empty strings("")
    :param vtex_account: name of the account providing Specification Field data - example 'ssesandbox04'
    :param specification_field_json: json payload as retreived from API https://developers.vtex.com/vtex-rest-api/reference/catalog-api-get-specification-field
    :return: string with SQL statement
    """

    return(f'INSERT INTO SpecificationField (account, Name, CategoryId, FieldId, IsActive, IsRequired, FieldTypeId, FieldValueId, FieldTypeName, Description, IsStockKeepingUnit, IsFilter, IsOnProductDetails, Position, IsWizard, IsTopMenuLinkActive, IsSideMenuLinkActive, DefaultValue, FieldGroupId, FieldGroupName) VALUES ("{vtex_account}","{str(specification_field_json["Name"] or "")}",{specification_field_json["CategoryId"] or ""},{str(specification_field_json["FieldId"] or "")},{str(specification_field_json["IsActive"] or "False")},{str(specification_field_json["IsRequired"] or "False")},{specification_field_json["FieldTypeId"] or ""},{specification_field_json["FieldValueId"] or "null"},"{str(specification_field_json["FieldTypeName"]) or ""}","{str(specification_field_json["Description"] or "")}",{str(specification_field_json["IsStockKeepingUnit"] or "False")},{str(specification_field_json["IsFilter"] or "False")},{str(specification_field_json["IsOnProductDetails"] or "True")},{str(specification_field_json["Position"] or "")},{str(specification_field_json["IsWizard"] or "False")},{str(specification_field_json["IsTopMenuLinkActive"] or "True")},{str(specification_field_json["IsSideMenuLinkActive"] or "True")},"{str(specification_field_json["DefaultValue"] or "")}",{str(specification_field_json["FieldGroupId"] or "")},"{str(specification_field_json["FieldGroupName"] or "")}")')

def convert_product_json_item_to_sql_insert(vtex_account, product_item_json):
    """ transforms a JSON object from VTEX Product API into 
        a valid SQL INSERT STATEMENT.
        string variables with value None will be converted into empty strings("")
        integer variables with value None will be converted into null
        removes quotes (") from Name and Descriptions
    :param vtex_account: name of the account providing Product data - example 'ssesandbox04'
    :param product_item_json: json payload as retreived from API https://developers.vtex.com/vtex-rest-api/reference/catalog-api-get-product
    :return: string with SQL statement
    """
    if (product_item_json['Name']) and (product_item_json['Name'].count('"')>0):
        product_item_json['Name'] = product_item_json['Name'].replace('"','')
    if (product_item_json['Description']) and (product_item_json['Description'].count('"')>0):
        product_item_json['Description'] = product_item_json['Description'].replace('"','')
    if (product_item_json['DescriptionShort']) and (product_item_json['DescriptionShort'].count('"')>0):
        product_item_json['DescriptionShort'] = product_item_json['DescriptionShort'].replace('"','')
    if (product_item_json['MetaTagDescription']) and (product_item_json['MetaTagDescription'].count('"')>0):
        product_item_json['MetaTagDescription'] = product_item_json['MetaTagDescription'].replace('"','')
    if (product_item_json['Title']) and (product_item_json['Title'].count('"')>0):
        product_item_json['Title'] = product_item_json['Title'].replace('"','')

    return(f'INSERT INTO Product (account,id,name,departmentId,categoryId,brandId,linkId,refId,isVisible,description,descriptionShort,releaseDate,keywords,title,isActive,taxCode,metaTagDescription,supplierId,showWithoutStock,AdWordsRemarketingCode,lomadeeCampaignCode,score) VALUES ("{vtex_account}","{product_item_json["Id"]}","{product_item_json["Name"]}",{product_item_json["DepartmentId"]},{product_item_json["CategoryId"]},{product_item_json["BrandId"]},"{product_item_json["LinkId"]}","{str(product_item_json["RefId"] or "")}",{product_item_json["IsVisible"]},"{str(product_item_json["Description"] or "")}","{str(product_item_json["DescriptionShort"] or "")}","{str(product_item_json["ReleaseDate"] or "")}","{str(product_item_json["KeyWords"] or "")}","{str(product_item_json["Title"] or "")}",{product_item_json["IsActive"]},"{str(product_item_json["TaxCode"] or "")}","{str(product_item_json["MetaTagDescription"] or "")}","{str(product_item_json["SupplierId"] or "")}",{product_item_json["ShowWithoutStock"]},"{str(product_item_json["AdWordsRemarketingCode"] or "")}","{str(product_item_json["LomadeeCampaignCode"] or "")}",{str(product_item_json["Score"] or "null")})')

def convert_sku_json_item_to_sql_insert(vtex_account, sku_item_json):
    """ transforms a JSON object from VTEX SKU API into 
        a valid SQL INSERT STATEMENT.
        string variables with value None will be converted into empty strings("")
        integer variables with value None will be converted into null
        removes quotes (") from Name
    :param vtex_account: name of the account providing SKU data - example 'ssesandbox04'
    :param product_item_json: json payload as retreived from API https://developers.vtex.com/vtex-rest-api/reference/catalog-api-get-sku
    :return: string with SQL statement
    """
    if (sku_item_json['Name']) and (sku_item_json['Name'].count('"')>0):
        sku_item_json['Name'] = sku_item_json['Name'].replace('"','')
        
    return(f'INSERT INTO Sku (account,id,productId,isActive,name,refId,PackagedHeight,PackagedLength,PackagedWidth,PackagedWeightKg,Height,Length,WeightKg,Width,CubicWeight,IsKit,CreationDate,RewardValue,EstimatedDateArrival,ManufacturerCode,CommercialConditionId,MeasurementUnit,UnitMultiplier,ModalType,KitItensSellApart,Videos) VALUES ("{vtex_account}","{sku_item_json["Id"]}","{sku_item_json["ProductId"]}",{sku_item_json["IsActive"]},"{sku_item_json["Name"]}","{sku_item_json["RefId"]}",{sku_item_json["PackagedHeight"]},{sku_item_json["PackagedLength"]},{sku_item_json["PackagedWidth"]},{sku_item_json["PackagedWeightKg"]},{str(sku_item_json["Height"] or "null")},{str(sku_item_json["Length"] or "null")},{str(sku_item_json["WeightKg"] or "null")},{str(sku_item_json["Width"] or "null")},{str(sku_item_json["CubicWeight"] or "null")},{str(sku_item_json["IsKit"] or "null")},"{sku_item_json["CreationDate"]}","{str(sku_item_json["RewardValue"] or "")}","{str(sku_item_json["EstimatedDateArrival"] or "")}","{str(sku_item_json["ManufacturerCode"] or "")}",{str(sku_item_json["CommercialConditionId"] or "null")},"{str(sku_item_json["MeasurementUnit"] or "")}",{str(sku_item_json["UnitMultiplier"] or "null")},"{str(sku_item_json["ModalType"] or "")}",{str(sku_item_json["KitItensSellApart"] or "false")},"{str(sku_item_json["Videos"] or "")}")')

def convert_image_json_item_to_sql_insert(vtex_account, image_item_json):
    """ transforms a JSON object from VTEX SKU API into 
        a valid SQL INSERT STATEMENT.
        string variables with value None will be converted into empty strings("")
        removes quotes (") from Name and Label 
    :param vtex_account: name of the account providing Image data - example 'ssesandbox04'
    :param product_item_json: json payload as retreived from API https://developers.vtex.com/vtex-rest-api/reference/catalog-api-get-sku-file
    :return: string with SQL statement
    """
    if (image_item_json['Name']) and (image_item_json['Name'].count('"')>0):
        image_item_json['Name'] = image_item_json['Name'].replace('"','')
    if (image_item_json['Label']) and (image_item_json['Label'].count('"')>0):
        image_item_json['Label'] = image_item_json['Label'].replace('"','')
        
    return(f'INSERT INTO Image (account,id,ArchiveId,SkuId,Name,IsMain,Label,Url) VALUES ("{vtex_account}","{image_item_json["Id"]}","{image_item_json["ArchiveId"]}",{image_item_json["SkuId"]},"{str(image_item_json["Name"] or "")}",{str(image_item_json["IsMain"] or "false")},"{str(image_item_json["Label"] or "")}","{str(image_item_json["Url"] or "")}")')

def convert_price_json_item_to_sql_insert(vtex_account, price_item_json):
    """ transforms a JSON object from VTEX SKU API into 
        a valid SQL INSERT STATEMENT.
        string variables with value None will be converted into empty strings("")
    :param vtex_account: name of the account providing Product data - example 'ssesandbox04'
    :param product_item_json: json payload as retreived from API https://developers.vtex.com/vtex-rest-api/reference/getprice
    :return: string with SQL statement
    """
    return(f'INSERT INTO Price (account,itemId,listPrice,costPrice,markup,basePrice,fixedPrices) VALUES ("{vtex_account}","{price_item_json["itemId"]}",{str(price_item_json["listPrice"] or "null")},{str(price_item_json["costPrice"] or "null")},{str(price_item_json["markup"] or "null")},{str(price_item_json["basePrice"] or "null")},"{str(price_item_json["fixedPrices"] or "")}")')

def convert_empty_price_to_sql_insert(vtex_account, price_item_id):
    """ returns an INSERT statement for a product without price info in the origin
    :param vtex_account: name of the account providing Product data - example 'ssesandbox04'
    :param price_item_id: SKU id to generate insert for
    :return: string with SQL statement
    """
    return(f'INSERT INTO Price (account,itemId,listPrice,costPrice,markup,basePrice,fixedPrices) VALUES ("{vtex_account}","{price_item_id}",null,001,000,001,"")')

def convert_source_spec_to_destination_spec(product_spec_payload):
    """ removes Id from product spec payload
    :param product_spec_payload: payload with Id
    :return: payload without Id member
    """
    new_dict_without_Id = product_spec_payload.copy()
    new_dict_without_Id[0].pop('Id',None)
    return(new_dict_without_Id)

def convert_source_sku_spec_to_destination_spec(sku_spec_payload):
    """ converts source specs to destination specs
    :param sku_spec_payload: original payload
    :return: payload with changed values
    """
    if type(sku_spec_payload)==bool:
        return ({
            "FieldId": 0,
            "FieldIdValueId": 0
        })
    new_dict_with_modified_values = sku_spec_payload.copy()
    new_dict_with_modified_values[0].pop('Id',None)
    new_dict_with_modified_values[0].pop('SkuId',None)
    new_dict_with_modified_values[0].pop('Text',None)
    if new_dict_with_modified_values[0]["FieldId"]:
        new_dict_with_modified_values[0]["FieldId"] = new_dict_with_modified_values[0]["FieldId"] - 1
    else:
        new_dict_with_modified_values[0]["FieldId"] = 0
    if new_dict_with_modified_values[0]["FieldValueId"]:    
        new_dict_with_modified_values[0]["FieldValueId"] = new_dict_with_modified_values[0]["FieldValueId"] + 10
    else:
        new_dict_with_modified_values[0]["FieldValueId"] = 0
    return(new_dict_with_modified_values[0])

## check all sqlite schemas...
check_sqlite_schema("Brand",schemaScriptBrand)
check_sqlite_schema("CategoryTree",schemaScriptCategoryTree)
check_sqlite_schema("Product",schemaScriptProduct)
check_sqlite_schema("Sku",schemaScriptSku)
check_sqlite_schema("Image",schemaScriptImage)
check_sqlite_schema("Price",schemaScriptPrice)
check_sqlite_schema("Category",schemaScriptCategory)
check_sqlite_schema("Specification",schemaScriptSpecification)
check_sqlite_schema("SpecificationField",schemaScriptSpecificationField)
check_sqlite_schema("SpecificationGroup",schemaScriptSpecificationGroup)
print()

if len(vtex_from_account_api_key)<10:
    vtex_api_from_account_cookies["VtexIdclientAutCookie"]= vtex_from_account_VtexIdclientAutCookie
else:
    vtex_api_from_account_headers["x-vtex-api-appkey"] = vtex_from_account_api_key
    vtex_api_from_account_headers["x-vtex-api-apptoken"] = vtex_from_account_api_token

if len(vtex_to_account_api_key)<10:
    vtex_api_to_account_cookies["VtexIdclientAutCookie"]= vtex_from_account_VtexIdclientAutCookie
else:
    vtex_api_to_account_headers["x-vtex-api-appkey"] = vtex_to_account_api_key
    vtex_api_to_account_headers["x-vtex-api-apptoken"] = vtex_to_account_api_token

#########################################################################################################
## CHECK CREDENTIALS AND ENVIRONMENT ACCESS TOKEN VIA SAMPLE API CALLS TO SOURCE AND DESTINATION ACCOUNTS
#########################################################################################################
if ((validate_token_on_account(vtex_api_from_account_cookies, vtex_api_from_account_headers, vtex_from_account,"SOURCE") == False) or (validate_token_on_account(vtex_api_to_account_cookies, vtex_api_to_account_headers, vtex_to_account,"DESTINATION") == False)):
    exit()

###########################################
# READ BRANDS FROM API AND SAVE TO DATABASE
###########################################
if skip_brands_step == True:
    if script_verbose_mode: print("SKIPPING BRANDS STEP")
else:
    print("==READING BRANDS===============================")
    source_number_of_pages = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers, f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog_system/pvt/brand/pagedlist?pageSize={str(BRANDS_API_PAGESIZE)}&page=1')['paging']['pages']

    if delete_brands_from_account: 
        clean_table_before_inserts(vtex_from_account,'Brand')
        if script_verbose_mode: print("SOURCE ACCOUNT BRANDS deleted from table")

    for page in range(1,source_number_of_pages+1):
        json_brand_items = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers,f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog_system/pvt/brand/pagedlist?pageSize={str(BRANDS_API_PAGESIZE)}&page={str(page)}')['items']
        brands_cursor = conn.cursor()
        for brand_item in json_brand_items:
            brands_cursor.execute(convert_brand_json_item_to_sql_insert(vtex_from_account, brand_item))
    conn.commit()      
    print("SOURCE ACCOUNT BRANDS COMMITED.")
            
    destination_number_of_pages = 0
    
    if delete_brands_to_account: 
        clean_table_before_inserts(vtex_to_account,'Brand')
        if script_verbose_mode: print("DESTINATION ACCOUNT BRANDS deleted from table")

    brands_cursor = conn.cursor()
    for page in range(1,destination_number_of_pages+1):
        json_brand_items = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers,f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog_system/pvt/brand/pagedlist?pageSize={str(BRANDS_API_PAGESIZE)}&page={str(page)}')['items']
        for brand_item in json_brand_items:
            brands_cursor.execute(convert_brand_json_item_to_sql_insert(vtex_to_account, brand_item))
    conn.commit()      
    print("DESTINATION ACCOUNT BRANDS COMMITED.")

###################################################
# READ CATEGORY TREES FROM API AND SAVE TO DATABASE
###################################################
if skip_category_tree_step == True:
    if script_verbose_mode: print("SKIPPING CATEGORY-TREE STEP")
else:
    print("==READING CATEGORY TREES=============================")
    json_source_category_tree = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers,f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog_system/pub/category/tree/{CATEGORIES_API_LEVELS}')

    if delete_category_tree_from_account: 
        clean_table_before_inserts(vtex_from_account,'CategoryTree')
        clean_table_before_inserts(vtex_to_account,'CategoryTree')
        if script_verbose_mode: print("SOURCE AND DESTINATION ACCOUNTS CATEGORY TREE deleted from table")

    def dumpChildrenAsSQL(categoryTreeCursor, categoryNode, vtex_account):
        if len(categoryNode['children'])==0:
            categoryTreeCursor.execute(convert_category_tree_json_item_to_sql_insert(vtex_account,categoryNode))
        else:
            categoryTreeCursor.execute(convert_category_tree_json_item_to_sql_insert(vtex_account,categoryNode))
            for nextNode in categoryNode['children']:
                dumpChildrenAsSQL(categoryTreeCursor, nextNode, vtex_account)

    category_tree_cursor = conn.cursor()    
    for rootNode in json_source_category_tree:
        dumpChildrenAsSQL(category_tree_cursor, rootNode, vtex_from_account)

    conn.commit()
    print("SOURCE ACCOUNT CATEGORY TREE COMMITED.")

    json_source_category_tree = read_api_as_JSON(vtex_api_to_account_cookies,vtex_api_to_account_headers,f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog_system/pub/category/tree/{CATEGORIES_API_LEVELS}')

    for rootNode in json_source_category_tree:
        dumpChildrenAsSQL(category_tree_cursor, rootNode, vtex_to_account)

    conn.commit()
    print("DESTINATION ACCOUNT CATEGORY TREE COMMITED.")

#################################################
# READ PRODUCT DATA FROM API AND SAVE TO DATABASE
#################################################
if skip_products_step == True:
    if script_verbose_mode: print("SKIPPING PRODUCTS STEP")
else:
    print("==READING PRODUCTS AND SKUS========================")
    if delete_products_from_account:
        clean_table_before_inserts(vtex_from_account,'Product')
        clean_table_before_inserts(vtex_from_account,'Sku')
        clean_table_before_inserts(vtex_from_account,'Image')
        clean_table_before_inserts(vtex_from_account,'Price')
        clean_table_before_inserts(vtex_from_account,'Specification')
        clean_table_before_inserts(vtex_from_account,'SpecificationGroup')
        clean_table_before_inserts(vtex_from_account,'SpecificationField')
        if script_verbose_mode: print("SOURCE ACCOUNT PRODUCTS, SKUS, IMAGES, PRICES, SPECIFICATIONS, SPECIFICATION GROUPS and SPECIFICATION FIELDS deleted from tables")

    product_cursor = conn.cursor()

    for categoryToRead in categoryArray:

        if script_verbose_mode: print(f"SCANNING CATEGORY ID: {categoryToRead}") 

        json_source_product_x_sku_ids = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers,f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds?_from=1&_to={PRODUCT_QUANTITY}&categoryId={categoryToRead}')

        count_all_products = len(json_source_product_x_sku_ids['data'])
        reading_product_list_position = 0
        printProgressBar(reading_product_list_position, count_all_products, prefix = 'Progress:', suffix = 'Complete', length = 50)

        for product_idx, current_product_id in enumerate(json_source_product_x_sku_ids['data']):
            ######## PROGRESS BAR 1
            reading_product_list_position = reading_product_list_position+1
            printProgressBar(reading_product_list_position, count_all_products, prefix = 'Progress:', suffix = 'Complete', length = 50)
            print()

            if script_verbose_mode: print(f"PRODUCT ID: {current_product_id} WITH SKU IDS: {json_source_product_x_sku_ids['data'][current_product_id]}") 
            json_source_product = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers, f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog/pvt/product/{str(current_product_id)}')
            
            product_cursor.execute(convert_product_json_item_to_sql_insert(vtex_from_account,json_source_product))
            for sku_idx, current_sku_id in enumerate(json_source_product_x_sku_ids['data'][current_product_id]):
                json_source_sku = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers, f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{str(current_sku_id)}')
                if json_source_sku:
                    product_cursor.execute(convert_sku_json_item_to_sql_insert(vtex_from_account,json_source_sku))
                    
                    json_source_sku_images = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers,f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{str(current_sku_id)}/file')
                    if json_source_sku_images:
                        for sku_image in json_source_sku_images:
                            sku_image['Url'] = f'https://{vtex_from_account}.vteximg.com.br/arquivos/ids/{str(sku_image["ArchiveId"])}/{str(sku_image["Name"])}.jpg'
                            product_cursor.execute(convert_image_json_item_to_sql_insert(vtex_from_account,sku_image))

                    json_source_sku_prices = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers, f"https://api.vtex.com/{vtex_from_account}/pricing/prices/{str(current_sku_id)}")
                    if json_source_sku_prices:
                        product_cursor.execute(convert_price_json_item_to_sql_insert(vtex_from_account,json_source_sku_prices))
                    else:
                        print(f'PRICE NOT FOUND FOR SKU ID={current_sku_id} at https://api.vtex.com/{vtex_from_account}/pricing/prices/{str(current_sku_id)} USING 0.01')
                        product_cursor.execute(convert_empty_price_to_sql_insert(vtex_from_account,current_sku_id))
            conn.commit()
   
    conn.commit()

####################################################################################
# READ DETAILED INFO FROM CATEGORIES COMING FROM PRODUCTS TABLE AND SAVE TO DATABASE
####################################################################################
if skip_categories_step == True:
    if script_verbose_mode: print("SKIPPING CATEGORIES STEP")
else:
    print("==READING CATEGORIES=============================")
    if delete_categories_from_account: 
        clean_table_before_inserts(vtex_from_account,'Category')
        clean_table_before_inserts(vtex_to_account,'Category')
        if script_verbose_mode: print("SOURCE AND DESTINATION ACCOUNTS CATEGORIES deleted from table")

    categories_cursor = conn.cursor()
    for (category_id,) in get_all_product_categories(vtex_from_account,vtex_to_account):
        json_source_category = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers,f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog/pvt/category/{category_id}')
        if json_source_category: categories_cursor.execute(convert_category_json_item_to_sql_insert(vtex_from_account, json_source_category))

        conn.commit()
    print("SOURCE ACCOUNT CATEGORIES COMMITED.")

    for (category_id,) in get_all_product_categories(vtex_from_account,vtex_to_account):
            json_destination_category = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers,f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/category/{category_id}')
            if json_destination_category: categories_cursor.execute(convert_category_json_item_to_sql_insert(vtex_to_account, json_destination_category))

            conn.commit()
    print("DESTINATION ACCOUNT CATEGORIES COMMITED.")
  
#########################
## PRODUCT CREATOR MAIN() 
#########################
if script_verbose_mode: print(f"CLEANING DATA FIELDS FROM PRODUCT TABLE")
cleaning_cursor = conn.cursor()
for query in cleanDataQueries:
    cleaning_cursor.execute(query)
conn.commit()

def getSKUImageJSON(img_id, img_ArchiveId, img_SkuId, img_Name, img_IsMain, img_Label, img_Url):
    return({
        "id": img_id,
        "ArchiveId": img_ArchiveId,
        "SkuId":img_SkuId,
        "Name":img_Name.replace('H03','').replace('._','_').replace('.2k',''),
        "IsMain":img_IsMain,
        "Label":img_Label.replace('H03','').replace('._','_').replace('.2k',''),
        "Url":img_Url.replace('H03','').replace('._','_').replace('.2k','')
    })

def getProductJSON(id, name, departmentId, categoryId, brandId, linkId, refId, isVisible, description, descriptionShort, releaseDate, keywords, title, isActive, taxCode, metaTagDescription, supplierId, showWithoutStock, adWordsRemarketingCode, lomadeeCampaignCode, score):
    return( {
        "id": id,
        "Name": name,
        "DepartmentId": departmentId,
        "CategoryId": categoryId,
        "BrandId": brandId,
        "LinkId": linkId,
        "RefId": refId,
        "IsVisible": isVisible,
        "Description": description,
        "DescriptionShort": descriptionShort,
        "ReleaseDate": releaseDate,
        "KeyWords": keywords,
        "Title": title,
        "IsActive": isActive,
        "TaxCode": taxCode,
        "MetaTagDescription": metaTagDescription,
        "SupplierId": supplierId,
        "ShowWithoutStock": showWithoutStock,
        "adWordsRemarketingCode": adWordsRemarketingCode,
        "lomadeeCampaignCode": lomadeeCampaignCode,
        "Score": score
    })

product_list_length = len(get_all_product_data(vtex_from_account))
product_list_position = 0
printProgressBar(product_list_position, product_list_length, prefix = 'Progress:', suffix = 'Complete', length = 50)

for (id, name, departmentId, categoryId, brandId, linkId, refId, isVisible, description, descriptionShort, releaseDate, keywords, title, isActive, taxCode, metaTagDescription, supplierId, showWithoutStock, adWordsRemarketingCode, lomadeeCampaignCode, score) in get_all_product_data(vtex_from_account):
    product_payload = getProductJSON(id, name, departmentId, categoryId, brandId, linkId, refId, isVisible, description, descriptionShort, releaseDate, keywords, title, isActive, taxCode, metaTagDescription, supplierId, showWithoutStock, adWordsRemarketingCode, lomadeeCampaignCode, score)

    print()
    ## PROGRESS BAR 2
    product_list_position = product_list_position+1
    printProgressBar(product_list_position, product_list_length, prefix = 'Progress:', suffix = 'Complete', length = 50)
    print()

    ## CHECK FOR STORE NAME IN PRODUCT NAME...
    if (name.upper().find(vtex_from_account_name.upper())) != -1:
        print(f'PRODUCT "{name}" MATCHES STORE NAME "{vtex_from_account_name}" SKIPPING')
        continue

    ## CHECKS IF CATEGORY IS READY FOR REPLICATION
    (isCategoryReady,categoryStatus,categoryMessage) = is_product_category_ready_on_destination(vtex_to_account,vtex_to_account,categoryId)
    if isCategoryReady: 
        if script_verbose_mode: print(f'CATEGORY#{categoryId} IS READY FOR REPLICATION!')
    else:
        if categoryStatus==0:
            print(f'CATEGORY NOT READY - {categoryMessage}')
            (new_category, responseStatus, responseText) = write_JSON_to_api(vtex_api_to_account_cookies,vtex_api_to_account_headers, f"https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/category", get_category_data_as_JSON(vtex_from_account,categoryId))
            if new_category:
                if script_verbose_mode: print(f'SUCCESSFULLY CREATED CATEGORY ID {categoryId} ON DESTINATION')
            else:
                print(f'ERROR CREATING CATEGORY ID {categoryId} ON DESTINATION: {responseStatus} - {responseText}')
                if (responseStatus==400) and (responseText=='{"Message":"Father category could not be found"}'):
                ## COULD IT BE A MISSING FATHER CATEGORY???
                    print("MISSING FATHER CATEGORIES...")
                    ############################
                    # CREATE UPSTREAM CATEGORIES
                    ############################                    
                    current_category_id = categoryId
                    categories_to_create_stack = []
                    while True:
                        categories_to_create_stack.append(current_category_id)
                        next_father_category_to_create = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers,f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog/pvt/category/{current_category_id}')['FatherCategoryId']
                        current_category_id = next_father_category_to_create
                        if current_category_id == None:
                            break; # reached top!

                    for category_id_to_create in categories_to_create_stack[::-1]:
                        categories_to_create_cursor = conn.cursor()
                        json_source_category_to_create = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers,f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog/pvt/category/{category_id_to_create}')
                        if json_source_category_to_create: categories_to_create_cursor.execute(convert_category_json_item_to_sql_insert(vtex_from_account, json_source_category_to_create))
                        conn.commit()
                        print("SOURCE ACCOUNT RECURSIVE CATEGORIES COMMITED.")

                        (new_stacked_category, responseStatus, responseText) = write_JSON_to_api(vtex_api_to_account_cookies,vtex_api_to_account_headers, f"https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/category", get_category_data_as_JSON(vtex_from_account,category_id_to_create))
                        if new_stacked_category:
                            if script_verbose_mode: print(f'SUCCESSFULLY RECURSIVELY CREATED CATEGORY ID {category_id_to_create} ON DESTINATION')
                        else:
                            print(f'UNABLE TO RECURSIVELY CREATE CATEGORY ID {category_id_to_create} ON DESTINATION - {responseStatus} - {responseText}')
                else:
                    print("NOT BECAUSE OF MISSING FATHER CATEGORIES... BETTER CHECK THIS... PAYLOAD MAYBE??? ABORTING!")
                    exit()
        else:
            print(f'CATEGORY NOT READY - {categoryMessage} - SKIPPING PRODUCT CREATION')
            continue

    ## CHECKS IF PRODUCT EXISTS ON DESTINATION
    json_destination_product = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers, f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/product/{product_payload["id"]}')
    if json_destination_product==False:
        if script_verbose_mode: print(f'PRODUCT#{product_payload["id"]} NEEDS TO BE CREATED ON DESTINATION')
        (new_product, responseStatus, responseText)  = write_JSON_to_api(vtex_api_to_account_cookies,vtex_api_to_account_headers, f"https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/product", product_payload)
        if new_product:
            print(f"[INFO] SUCCESSFULLY CREATED PRODUCT {product_payload['Name']}")
        else:
            print(f"[ERROR] CREATING PRODUCT {product_payload['Name']} - {responseStatus} - {responseText}")
    else:
        if script_verbose_mode: print(f'PRODUCT#{product_payload["id"]} ALREADY EXISTS ON DESTINATION!')
        if (product_payload['Name'] == json_destination_product['Name']):
            print(f"AND BOTH NAMES ARE: {json_destination_product['Name']}")
        else:
            print(f"MISMATCHED PRODUCTS: {product_payload['Name']} ON SOURCE DOES NOT MATCH {json_destination_product['Name']} ON DESTINATION!!! SKIPPING.")
            continue

    #################################
    #### PRODUCT SPECIFICATIONS CHECK
    #################################
    ### TODO: fazer funcionar para multiplas especificacoes... Por enquanto assume que é a Cor e pega os valores
    json_source_product_specs = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers, f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog_system/pvt/products/{product_payload["id"]}/specification')
    # print(f'json_source_product_specs={json_source_product_specs}')
    json_destination_product_specs = read_api_as_JSON(vtex_api_to_account_cookies,vtex_api_to_account_headers, f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog_system/pvt/products/{product_payload["id"]}/specification')
    # print(f'json_destination_product_specs={json_destination_product_specs}')
    if json_destination_product_specs:
        if (json_destination_product_specs[0]['Value'][0] == json_source_product_specs[0]['Value'][0]):
            print(f'SPEC (Cor) ALREADY THERE, BOTH WITH VALUE=({json_destination_product_specs[0]["Value"][0]})')
        else:
            print(f'SPEC (Cor) ALREADY THERE, BUT DIFFERENT VALUES:({json_source_product_specs[0]["Value"][0]}) AND ({json_destination_product_specs[0]["Value"][0]})')
    else:
        print(f'MISSING SPEC (Cor), WILL CREATE ON DESTINATION...')
        json_destination_product_spec_create = write_JSON_to_api(vtex_api_to_account_cookies,vtex_api_to_account_headers,f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog_system/pvt/products/{product_payload["id"]}/specification', convert_source_spec_to_destination_spec(json_source_product_specs))
        if json_destination_product_spec_create:
            print(f"[INFO] SUCCESSFULLY CREATED SPEC COR")
        else:
            print(f"[ERROR] CREATING SPEC COR - {responseStatus} - {responseText}")
    
    ## CHECKS IF SKU EXISTS ON DESTINATION
    for (skuid,) in get_product_sku_ids(vtex_from_account,id):
        json_destination_sku = read_api_as_JSON(vtex_api_to_account_cookies,vtex_api_to_account_headers, f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{skuid}')
        
        if json_destination_sku==False:
            if script_verbose_mode: print(f'SKU#{skuid} NEEDS TO BE CREATED ON DESTINATION')
            (new_sku, responseStatus, responseText)  = write_JSON_to_api(vtex_api_to_account_cookies,vtex_api_to_account_headers, f"https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit", get_sku_data_as_JSON(vtex_from_account,skuid))
            if new_sku:
                print(f"[INFO] SUCCESSFULLY CREATED SKU {get_sku_data_as_JSON(vtex_from_account,skuid)['Name']}")
            else:
                print(f"[ERROR] CREATING SKU {get_sku_data_as_JSON(vtex_from_account,skuid)['Name']} - {responseStatus} - {responseText}")
        else:
            if script_verbose_mode: print(f'SKU#{skuid} ALREADY EXISTS ON DESTINATION!')
            if (get_sku_data_as_JSON(vtex_from_account,skuid)['Name'] == json_destination_sku['Name']):
                print(f"AND BOTH NAMES ARE: {json_destination_sku['Name']}")
            else:
                print(f"MISMATCHED SKUS: {get_sku_data_as_JSON(vtex_from_account,skuid)['Name']} ON SOURCE DOES NOT MATCH {json_destination_sku['Name']} ON DESTINATION!!! SKIPPING.")
                continue

        #############################
        #### SKU SPECIFICATIONS CHECK
        #############################
        ### TODO: fazer funcionar para multiplas especificacoes... Por enquanto assume que é a Tamanho e pega os valores
        ### vai ter que criar uma regra que busca o texto equivalente nos atributos de SKU criados no destino
        ### aparentemente nao da para criar os mesmos ids de atributos ou associar pelo texto!!! DAMN.
        ### Por enquanto, vai precisar criar um DE-PARA:
        ###
        ###  bravtexfashionstore              <=> ssesanbox03
        ###  FieldId=23,FieldValueId=86 (PP)      FieldId=24,FieldValueId=87 (PP) (add one???)
        ###  para o B2B, parece ser o contrario, vou subtrair um!!!
        json_source_sku_specs = read_api_as_JSON(vtex_api_from_account_cookies,vtex_api_from_account_headers, f'https://{vtex_from_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{skuid}/specification')
        json_destination_sku_specs = read_api_as_JSON(vtex_api_to_account_cookies,vtex_api_to_account_headers, f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{skuid}/specification')
        if json_destination_sku_specs:
            if (json_destination_sku_specs[0]['FieldId'] == json_source_sku_specs[0]['FieldId']):
                print(f'SPEC (TAMANHO) ALREADY THERE, BOTH WITH VALUE=({json_destination_sku_specs[0]["FieldId"]})')
            else:
                print(f'SPEC (TAMANHO) ALREADY THERE, BUT DIFFERENT VALUES:({json_destination_sku_specs[0]["FieldId"]}) AND ({json_source_sku_specs[0]["FieldId"]})')
        else:
            print(f'MISSING SPEC (TAMANHO), WILL CREATE ON DESTINATION...')
            (json_destination_sku_spec_create,responseStatus,responseMessage)  = write_JSON_to_api(vtex_api_to_account_cookies,vtex_api_to_account_headers,f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{skuid}/specification', convert_source_sku_spec_to_destination_spec(json_source_sku_specs))
            if json_destination_sku_spec_create:
                print(f"[INFO] SUCCESSFULLY CREATED SKU SPEC TAMANHO")
                #  SENDING PAYLOAD {convert_source_sku_spec_to_destination_spec(json_source_sku_specs)} TO ENDPOINT https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{skuid}/specification")
            else:
                print(f"[ERROR] CREATING SPEC TAMANHO - {responseStatus} - {responseText}")
                print(f'https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{skuid}/specification')
                print(f'{convert_source_sku_spec_to_destination_spec(json_source_sku_specs)}')
        
        ## DELETE ALL FILES ON DESTINATION SKU
        sku_id_to_delete_all_files = skuid;
        (deleted_images_on_destination,deleteStatus,deleteMessage) = send_DELETE_to_api(vtex_api_to_account_cookies,vtex_api_to_account_headers, f"https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{sku_id_to_delete_all_files}/file", payload="")

        if deleted_images_on_destination:
            # RECREATE ALL IMAGES
            for (img_id, img_ArchiveId, img_SkuId, img_Name, img_IsMain, img_Label, img_Url, ) in get_sku_images(vtex_from_account,skuid):
                (new_sku_file,responseStatus,responseMessage) = write_JSON_to_api(vtex_api_to_account_cookies,vtex_api_to_account_headers,f"https://{vtex_to_account}.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{str(skuid)}/file", getSKUImageJSON(img_id, img_ArchiveId, img_SkuId, img_Name, img_IsMain, img_Label, img_Url))
                if new_sku_file:
                    if script_verbose_mode: print(f'SKU IMAGE CREATED SUCESSFULLY FOR SKU ID={skuid}')
                else:
                    print(f'ERROR CREATING IMAGE FOR SKU {skuid}: {responseStatus} - {responseMessage}')
                    print(f'getSKUImageJSON={getSKUImageJSON(img_id, img_ArchiveId, img_SkuId, img_Name, img_IsMain, img_Label, img_Url)}')

        # TODO: review franchise accounts strategy??? For now force destination account as warehouse to fill in
        ## STOCK AND PRICE CREATION (destination_warehouse_id)
        for franchise_account_to_send_stock in franchise_accounts:
            if franchise_warehouse_id:
                (new_sku_inventory, responseStatus, responseMessage) = update_JSON_to_api(vtex_api_to_account_cookies,vtex_api_to_account_headers,f"https://{franchise_account_to_send_stock}.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/{str(skuid)}/warehouses/{franchise_warehouse_id}",get_sample_sku_stock_JSON())
                if new_sku_inventory:
                    if script_verbose_mode: print(f'SKU STOCK CREATED SUCESSFULLY IN WAREHOUSE ID={franchise_account_to_send_stock}|{franchise_warehouse_id}')
                else:
                    print(f'ERROR CREATING STOCK FOR SKU {skuid} in WAREHOUSE {franchise_account_to_send_stock}|{franchise_warehouse_id}: {responseStatus} - {responseMessage}')
                    print(f'get_sample_sku_stock_JSON()={get_sample_sku_stock_JSON()} sent to ')
                    print(f'URL=https://{franchise_account_to_send_stock}.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/{str(skuid)}/warehouses/{franchise_warehouse_id}')

        for franchise_account_to_send_price in franchise_accounts:
            (new_sku_price, responseStatus, responseMessage) = update_JSON_to_api(vtex_api_to_account_cookies,vtex_api_to_account_headers,f"https://{franchise_account_to_send_price}.vtexcommercestable.com.br/api/pricing/prices/{str(skuid)}",get_sku_price_as_JSON(vtex_from_account,skuid))
            if new_sku_price:
                if script_verbose_mode: print(f"PRICE CREATED SUCESSFULLY FOR SKU ID={skuid} IN {franchise_account_to_send_price}")
            else:
                print(f"ERROR CREATING PRICE FOR SKU {skuid}: {responseStatus} - {responseMessage} in {franchise_account_to_send_price}")
                print(f'get_sku_price_as_JSON={get_sku_price_as_JSON(vtex_from_account,skuid)} sent to')
                print(f"https://{franchise_account_to_send_price}.vtexcommercestable.com.br/api/pricing/prices/{str(skuid)}")
    
