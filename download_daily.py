#encoding=utf8
import json

import datetime
import sys
import time
import json
from websocket import create_connection
import config



if __name__=='__main__':
  ws = create_connection("wss://www.roofstock.com/api/signalr/connect?transport=webSockets&clientProtocol=1.5&Bearer=&connectionToken=Bm%2BJpXG0plE6tRrcEirFbypzyiU%2F0Y8T8dJ%2FkzBhRlUuDD8Z%2BMyxhTv5HhJdCY29MYKxVAppb7Gglh4W33SuMB%2FUbCAcLGyvBgadyAwacVKhiCuj&connectionData=%5B%7B%22name%22%3A%22propertyviewshub%22%7D%5D&tid=8")
  ws.send('{"H":"propertyviewshub","M":"createOrUpdate","A":[{"parent":null,"itemType":"MarketCategory","name":"All Categories","items":null,"hasItem":true,"meta":null,"summary":null,"query":{"where":[{"field":"property.market","range":null,"values":null,"negate":false},{"field":"property.financial.listPrice","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.computed.grossYield","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.computed.monthlyRent","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.financial.monthlyRent","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.financial.marketRent","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.computed.appreciation","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.computed.priceDiscountComparedToMarketValuation","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.computed.monthsLeftOnLease","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.financial.isMarketRentGreater","range":null,"values":null,"negate":false},{"field":"property.daysSinceListing","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.score.averageSchoolScore","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.score.neighborhoodScore","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.physical.bedRooms","range":{"min":0,"max":6},"values":null,"negate":false},{"field":"property.physical.bathRooms","range":{"min":0,"max":5},"values":null,"negate":false},{"field":"property.physical.squareFeet","range":{"min":0,"max":5000},"values":null,"negate":false},{"field":"property.physical.yearBuilt","range":{"min":0,"max":2017},"values":null,"negate":false},{"field":"property.computed.risk","range":{"min":null,"max":null},"values":null,"negate":false},{"field":"property.financial.isHoa","range":null,"values":null,"negate":false},{"field":"property.isInspectionTypeExteriorOnly","range":null,"values":null,"negate":false},{"field":"property.financial.isSection8","range":null,"values":null,"negate":false},{"field":"property.computed.isBestSchools","range":null,"values":null,"negate":false},{"field":"property.isFeatured","range":null,"values":null,"negate":false},{"field":"property.isRentGuaranteed","range":null,"values":null,"negate":false},{"field":"property.financial.isCashOnly","range":null,"values":null,"negate":false},{"field":"property.isBargain","range":null,"values":[false],"negate":false},{"field":"property.status","range":null,"values":["ForSale","SalePending","Sold"],"negate":false},{"field":"property.visibility","range":null,"values":["Public"],"negate":false},{"field":"property.physical.isPool","range":null,"values":null,"negate":false}],"orderBy":[{"field":"property.computed.sortOrder.marketplace","direction":"Asc"},{"field":"property.computed.sortOrder.random","direction":"Asc"}],"select":[{"field":"property.computed.totalReturn"},{"field":"property.*"},{"field":"property.address.*"},{"field":"property.physical.*"},{"field":"property.financial.*"},{"field":"property.score.*"},{"field":"property.valuation.*"},{"field":"property.computed.leveredIrr"},{"field":"property.computed.unleveredAnnualCashFlow"},{"field":"property.computed.unleveredNetYield"},{"field":"property.computed.grossYield"},{"field":"property.computed.isBestSchools"},{"field":"property.computed.monthsLeftOnLease"},{"field":"property.computed.priceDiscountComparedToMarketValuation"},{"field":"property.computed.monthlyRent"},{"field":"property.computed.appreciation"},{"field":"property.computed.salePrice"}],"summary":[]},"pagination":null,"valueSets":{"default":{"values":[{"field":"usecurrentrent","fieldType":"Percentage","value":true},{"field":"view.randomseed","type":"Text","value":"marketplace:1515489327772"}]}},"sourceId":"1","sourceType":"Market","scheduleType":null,"type":"Predefined","id":"marketplace"},false,{"href":"https://www.roofstock.com/investment-property-marketplace?dv=list"}],"I":1}')

  while 1==1:
    result = ws.recv()
    if result=='{}':
      ws.close()
      sys.exit()
    try:
      roomlist=json.loads(result)['R']['items']

      # print count to screen
      total=len(roomlist)
      print("Total rooms:{}".format(total))

      # dump to csv
      with open(config.DAILY_PATH + 'room_json_{}.txt'.format(datetime.date.today().isoformat()),'w') as f:
        json.dump(roomlist,f)

    except Exception as e:
      print(e)
