#script to fetch data from one collections  from database and insert it into the another database collection
import pymongo
import googlemaps
import time

# Connection to the database
client = pymongo.MongoClient("connection")
db = client["dbname"]
col = db["col_name"]
dbb = client["db2name"]
cols = dbb["col2_name"]
ssss=" "
llll=" "
rr=" "
pppp=" "
ccc= " "

# Code to fetch all documents from the 'staging/usfederal'

x=db.usfederal.find()

for data in x:
    dd=data['Tender']['Tenderers']['Address']

    # code to fetch address from the Tenderers field.

    if(len(dd)!= 0):
        add_dct={"street":0,"local":0,"region":0,"postal":0,"cntry":0}
        add=[]
        add.append(data['Tender']['Tenderers']['Address'])
        
        if (add!=" "):

            try:
                for i in add:
                    try:
                        if(i['Street Address']!=" "):
                            street=i['Street Address']
                            add_dct.update({"street":1})

                    except KeyError:
                            street=" "
                   
                    try:
                        if(i['Locality']!=" "):
                            local=i['Locality']
                            add_dct.update({"local":1})
                    except KeyError:
                            local=" "
                    try:
                        if(i['Region']!=" "):
                            region=i['Region']
                            add_dct.update({"region":1})

                    except KeyError:
                            region=" "
                    try:
                        if(i['Postal Code']!=" "):
                            postal=i['Postal Code']
                            add_dct.update({"postal":1})
                    except KeyError:
                            postal=" "
                    
                    try:
                        if(i['Country Name']!=" "):
                            cntry=i['Country Name']
                            add_dct.update({"cntry":1})
                    except KeyError:
                            cntry=" "
                    
                    final_pass=[]

                    if(add_dct["street"]==1):
                        ssss=street
                        final_pass.append(street)

                    if(add_dct["local"]==1):
                        llll=local
                        final_pass.append(local)
                    if(add_dct["region"]==1):
                        rr=region
                        final_pass.append(region)
                    if(add_dct["postal"]==1):
                        pppp=postal
                        final_pass.append(postal)

                    if(add_dct["cntry"]==1):
                        ccc=cntry
                        final_pass.append(cntry)

                   # code to generate the Longitude and Latitude
                    gmaps = googlemaps.Client(key='enter keyyyy')
                    geocode_result = gmaps.geocode(final_pass)

                    # code to filter the longitude and latitude from geocoe_result

                    if len(geocode_result)!=0:
                        for x in geocode_result:
                            lats=x['geometry']['location']['lat']
                            lngs=x['geometry']['location']['lng']
                            data["Tender"]['Tenderers']['Address'].update({"Longitude":lngs,"Latitude":lats})
                            
                            try:
                                
                                client = pymongo.MongoClient("connection")
                                dbb = client["dbname"]
                                cols = dbb["col_name"]

                                #Code to insert data to the database allrecords
                                alldata=dbb.allrecords.find()

                                # Code to update document with existing ocid 
                                if dbb.allrecords.count_documents({ 'OCID': data['OCID'] }, limit = 1):
                                   
                                    pp=dbb.allrecords.find_one({ 'OCID': data['OCID']})
                                    dt=pp['Tender']['Tenderers']['Address']
                                    dt.update({"Longitude":lngs,"Latitude":lats}) 
                                    x=dbb.allrecords.update_one({'OCID': data['OCID'] },{ "$set":{'Tender': {'Tender ID':pp['Tender']['Tender ID'], 'Tender Title':pp['Tender']['Tender Title'], 'Tender Description':pp['Tender']['Tender Description'] , 'Procuring Entity':pp['Tender']['Procuring Entity'], 'Enquiry Period': pp['Tender']['Enquiry Period'], 'Tenderers':pp['Tender']['Tenderers'] }} } )
                                    ssp=data['_id']
                                    db.usfederal.delete_one({'_id':ssp})

                                else:
                                    # Code to insert document with longitude and latitude

                                    dbb.allrecords.insert_one(data)
                                    ssp=data['_id']                                    
                                    db.usfederal.delete_one({'_id':ssp})        
                            except:
                                
                                dbb.allrecords.insert_one(data)
                                ssp=data['_id']                                    
                                db.usfederal.delete_one({'_id':ssp})                   
                    else:
                        # Code to save document without geocode
                        dbb.allrecords.insert_one(data)                       
                        ssp=data['_id']
                        db.usfederal.delete_one({'_id':ssp})
                        


            except KeyError:
                c="noneall"
                print(c)
        
    else:
        if dbb.allrecords.count_documents({"_id": data["_id"]}) == 0:
            # Code to insert document without address
            client = pymongo.MongoClient("conection")
            dbb = client["dbname"]
            cols = dbb["col_name"]
            db = client["db2name"]
            col = db["col2_name"]            
            dbb.allrecords.insert_one(data)
            ssp=data['_id']
            db.usfederal.delete_one({'_id':ssp})





