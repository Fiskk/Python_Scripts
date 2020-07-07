# Testing SOAP API calls with Python
import requests
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc


def Prettify(rough_string): # Prints out an XML formatted to Output
    """ Return a pretty-printed XML string """
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def pretty_string_to_file(xml_string,file_fullpath):
    """ Output txt file in xml format at path """
    file = open(file_fullpath, "w")
    file.writelines(Prettify(xml_string.content)) 
    file.close()


def string_to_xml(xml_string):
    """ Convert xml in string format to xml etree """
    xml = ET.fromstring(xml_string)
    return xml


def send_to_sql_server(dataframe):
    """ Connect to server, load dataframe into table """
    # TODO this should utilize a context-manager
    # TODO This is only connecting to DEV, notice the server param
    connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                "Server=BISQLDEV;"
                                "Database=NSightStaging;"
                                "Trusted_Connection=yes;")
    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE [Extract].[SupplyPro.Inventory]")
    for index, row in dataframe.iterrows():
        
        cursor.execute("INSERT INTO [Extract].[SupplyPro.Inventory]([DeviceID], [DeviceName], [ProductID], [PartNumber],\
                                                                [ProductName], [AltPartNumber], [AltPartNumber2], [AltPartNumber3],\
                                                                [PrimaryVendorCode], [Quantity]) \
                         values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row['DeviceID'],
                                                                row['DeviceName'],
                                                                row['ProductID'],
                                                                row['PartNumber'],
                                                                row['ProductName'],
                                                                row['AltPartNumber'],
                                                                row['AltPartNumber2'],
                                                                row['AltPartNumber3'],
                                                                row['PrimaryVendorCode'],
                                                                row['Quantity'])

        connection.commit()
        # break
    # cursor.execute('SELECT top 5 * FROM [Extract].[SupplyPro.Inventory]')
    # for row in cursor:
    #     print('row = %r' % (row,))

    cursor.close()
    connection.close()


def tree_to_dataframe(tree):
    """ Parses the passed in xml tree and inserts it into an array then appends that array to a df """
    df = pd.DataFrame(columns=['DeviceID', 'DeviceName', 'ProductID', 'PartNumber',
                               'ProductName', 'AltPartNumber', 'AltPartNumber2',
                               'AltPartNumber3', 'PrimaryVendorCode', 'Quantity'])

    # Placeholder values
    row = ['X', 'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X', ]
    for child in tree.iter('{SupplyPro.DataUploader.Serialization}DeviceIdentificationXML'):

        DeviceID = row[0] = (child.find('{SupplyPro.DataUploader.Serialization}DeviceID')).text
        DeviceName = row[1] = (child.find('{SupplyPro.DataUploader.Serialization}DeviceName')).text

        for child2 in child:

            for child3 in child2.iter('{SupplyPro.DataUploader.Serialization}InventoryDetailXML'):

                ProductID = row[2] = (child3.find('{SupplyPro.DataUploader.Serialization}ProductID')).text
                PartNumber = row[3] = (child3.find('{SupplyPro.DataUploader.Serialization}PartNumber')).text
                ProductName = row[4] = (child3.find('{SupplyPro.DataUploader.Serialization}ProductName')).text
                AltPartNumber = row[5] = (child3.find('{SupplyPro.DataUploader.Serialization}AltPartNumber')).text
                AltPartNumber2 = row[6] = (child3.find('{SupplyPro.DataUploader.Serialization}AltPartNumber2')).text
                AltPartNumber3 = row[7] = (child3.find('{SupplyPro.DataUploader.Serialization}AltPartNumber3')).text
                PrimaryVendorCode = row[8] = (child3.find('{SupplyPro.DataUploader.Serialization}PrimaryVendorCode')).text
                Quantity = row[9] = (child3.find('{SupplyPro.DataUploader.Serialization}Quantity')).text

                # print(DeviceID)
                # print(DeviceName)
                # print(ProductID)
                # print(PartNumber)
                # print(ProductName)
                # print(AltPartNumber)
                # print(AltPartNumber2)
                # print(AltPartNumber3)
                # print(PrimaryVendorCode)
                # print(Quantity)

                # print(row)
                df = df.append(pd.Series(row, index=df.columns), ignore_index=True)

                # break

    # This is just here so you can be sure data is in the dataframe
    # It's not functionaly relevent            
    # print(df.head(5))

    return df


def main():
    """ Main """

    # TODO These variables have hard-coded auth info in them, should be parameterized
    Username = ""
    Password = ""
    CompanyCode = ""
    SecurityKey = ""

    # This is the request body that is sent to the API, the 'u' states that it's a unicode string
    request = u"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Header>
        <AuthHeader xmlns="http://SupplyPro.Web.Service/DirectInterface">
        <Username>{0}</Username>
        <Password>{1}</Password>
        <CompanyCode>{2}</CompanyCode>
        <SecurityKey>{3}</SecurityKey>
        </AuthHeader>
    </soap:Header>
    <soap:Body>
        <GetCurrentInventoryBySite xmlns="http://SupplyPro.Web.Service/DirectInterface" />
    </soap:Body>
    </soap:Envelope>""".format(Username, Password, CompanyCode, SecurityKey)
                
    # print(request)
    encoded_request = request.encode('utf-8')

    headers = {"Host": "port3ws.supplypro.com",
               "Content-Type": "text/xml; charset=utf-8",
               "Content-Length": str(len(encoded_request)),
               "SOAPAction": "http://SupplyPro.Web.Service/DirectInterface/GetCurrentInventoryBySite"}

    response = requests.post(url="https://port3ws.supplypro.com/DirectInterface.asmx?WSDL",
                             headers = headers,
                             data = encoded_request)

    # print(response)
    final_xml = string_to_xml(response.content)
    # # pretty_string_to_file(response.content, r'C:\Users\ssampson\Documents\Python_Scripts\SOAP_TXT_Test.txt')
    final_dataframe = tree_to_dataframe(final_xml)
    send_to_sql_server(final_dataframe)
    
main()