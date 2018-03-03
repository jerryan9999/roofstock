import json
import psycopg2
import datetime
import config

conn = psycopg2.connect(config.SQL_STR)
cursor = conn.cursor()

master_ref = {
    "source_id":{"level":0,"upper":None,"key":"id"},
    "latitude":{"level":0,"upper":None,"key":"latitude"},
    "longitude":{"level":0,"upper":None,"key":"longitude"},
    "squarefeet":{"level":1,"upper":"physical","key":"squareFeet"},

    "bathrooms":{"level":1,"upper":"physical","key":"bathRooms"},
    "bedrooms":{"level":1,"upper":"physical","key":"bedRooms"},
    "yearbuilt":{"level":1,"upper":"physical","key":"yearBuilt"},
    "propertytype":{"level":0,"upper":None,"key":"propertyType"},
    "lotsize":{"level":1,"upper":"physical","key":"lotSize"},
    "ispool":{"level":1,"upper":"physical","key":"isPool"},
    "address1":{"level":1,"upper":"address","key":"address1"},
    "zip":{"level":1,"upper":"address","key":"zip"},
    "city":{"level":1,"upper":"address","key":"city"},
    "county":{"level":1,"upper":"address","key":"county"},
    "cbsacode":{"level":0,"upper":None,"key":"cbsaCode"},
    "state":{"level":1,"upper":"address","key":"state"},

    "listprice":{"level":1,"upper":"financial","key":"listPrice"},
    "monthlyrent":{"level":1,"upper":"financial","key":"monthlyRent"},
    "yearlyinsurancecost":{"level":1,"upper":"financial","key":"yearlyInsuranceCost"},
    "yearlypropertytaxes":{"level":1,"upper":"financial","key":"yearlyPropertyTaxes"},
    "appreciation":{"level":1,"upper":"computed","key":"appreciation"},
    "neighborscore":{"level":1,"upper":"score","key":"neighborhoodScore"},
    "score":{"level":None,"upper":None,"key":None},

    "status":{"level":0,"upper":None,"key":"status"},
    "imgurl":{"level":0,"upper":None,"key":"mainImageUrl"}
}


def strip_key_info(house):
  item = {}
  for k,v in master_ref.items():
    if v['level']==0:
      item[k] = house[v['key']] 
    elif v['level']==1:
      item[k] = house[v['upper']][v['key']]
  return item

def update2_db(item):
  sql = """INSERT INTO property(
              source,
              source_id,
              latitude,
              longitude,
              squarefeet,

              bathrooms,
              bedrooms,
              yearbuilt,
              propertytype,
              lotsize,
              ispool,
              address1,
              zip,
              city,
              county,
              cbsacode,
              state,

              listprice,
              monthlyrent,
              yearlyinsurancecost,
              yearlypropertytaxes,
              appreciation,
              status,

              created_at,
              updated_at,
              neighborscore,
              imgurl
      )
      VALUES (
              %(source)s,
              %(source_id)s,
              %(latitude)s,
              %(longitude)s,
              %(squarefeet)s,

              %(bathrooms)s,
              %(bedrooms)s,
              %(yearbuilt)s,
              %(propertytype)s,
              %(lotsize)s,
              %(ispool)s,
              %(address1)s,
              %(zip)s,
              %(city)s,
              %(county)s,
              %(cbsacode)s,
              %(state)s,

              %(listprice)s,
              %(monthlyrent)s,
              %(yearlyinsurancecost)s,
              %(yearlypropertytaxes)s,
              %(appreciation)s,
              %(status)s,

              %(created_at)s,
              %(updated_at)s,
              %(neighborscore)s,
              %(imgurl)s

      )
      ON CONFLICT ON CONSTRAINT unique_property_constraint
      DO UPDATE SET
        status=%(status)s,
        updated_at=%(updated_at)s,
        listprice=%(listprice)s,
        score_v1_appreciation = %(score_v1_appreciation)s,
        score_v2_balance = %(score_v2_balance)s,
        score_v3_return = %(score_v3_return)s,
        score_version = %(score_version)s
    """
  cursor.execute(sql,item)
  conn.commit()

def sql_command():
  # fill up neighbor_regionid
  sql = """
    UPDATE property 
    SET 
      neighbor_regionid=(SELECT cast(regionid AS integer) FROM zillownh WHERE ST_CONTAINS(geom,ST_GeomFromText('POINT('||longitude::text||' '||latitude::text||')')))
  """
  cursor.execute(sql)
  conn.commit()

  # fill up neighbor score from index table
  sql = """
      UPDATE property SET neighborscore = neighborhood_index.neighborhood_score 
      FROM neighborhood_index 
      WHERE property.neighbor_regionid = neighborhood_index.regionid
    """
  cursor.execute(sql)
  conn.commit()

def max_paras():
  sql = """
    SELECT MAX(monthlyrent*12/listprice) as ratio_max,
     MAX(bedrooms*0.3+bathrooms*0.2) as property_max,
     MAX(appreciation) as appreciation_max,
     MAX(neighborscore) as neighborscore_max,
     MAX(yearlyinsurancecost+yearlypropertytaxes) as cost_max
    FROM property
    WHERE source = 'roofstock'
  """
  cursor.execute(sql)
  _value = cursor.fetchone()
  return {"ratio_max":_value[0],
          "property_max":_value[1],
          "appreciation_max":_value[2],
          "neighborscore_max":_value[3],
          "cost_max":_value[4]
         }


def score_process(item,para_max):
  rental_sale_ratio = item['monthlyrent']*12/item['listprice']/para_max['ratio_max']
  property_score = item['bedrooms']*0.3+item['bathrooms']*0.2/para_max['property_max']
  try:
    cost = (item.get('yearlyinsurancecost')+item.get('yearlypropertytaxes'))/para_max['cost_max']
  except:
    item['score_v1_appreciation'] = None
    item['score_v2_balance'] = None
    item['score_v3_return'] = None
    return item
  
  item['score_v1_appreciation'] = 0.6*item['appreciation']/para_max['appreciation_max']+0.2*rental_sale_ratio+0.2*property_score-0.2*cost+0.2*item['neighborscore']/para_max['neighborscore_max']
  item['score_v2_balance'] = 0.4*item['appreciation']/para_max['appreciation_max']+0.4*rental_sale_ratio+0.2*property_score-0.2*cost+0.2*item['neighborscore']/para_max['neighborscore_max']
  item['score_v3_return'] = 0.2*item['appreciation']/para_max['appreciation_max']+0.6*rental_sale_ratio+0.2*property_score-0.2*cost+0.2*item['neighborscore']/para_max['neighborscore_max']

  return item

if __name__ == '__main__':

  today = datetime.date.today().isoformat()
  daily_csv = config.DAILY_PATH +'room_json_{}.txt'.format(today)
  with open(daily_csv) as f:
    properties = json.load(f)
  para_max = max_paras()

  for p in properties:
    item = strip_key_info(p['property'])
    item['source'] = "roofstock"
    item['neighborscore'] = 20*float(item['neighborscore']) if item['neighborscore'] is not None else 20*3
    item = score_process(item,para_max)
    now = datetime.datetime.now()
    item['created_at'] = now
    item['updated_at'] = now

    item['score_version'] = 0
    update2_db(item)
