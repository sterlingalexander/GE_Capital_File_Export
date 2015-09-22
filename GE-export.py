import sys
import pyodbc
import time
import subprocess
import shutil
import os
import ConfigParser

# Set to 1 or 10 to get extra output to the console
DEBUG = 0

# Check to see if we had a date passed in
if len(sys.argv) > 1:
    query_date = sys.argv[1]
else:
    query_date = time.strftime("%Y%m%d")

if DEBUG:
    print query_date

# Parse the configuration file and set the config values
cfg = ConfigParser.ConfigParser()
cfg.read('config.ini')

#==========| Configuration strings |==========
try:
    db_latitude_ip = cfg.get("Database", "db_latitude_ip")
    db_latitude_name = cfg.get("Database", "db_latitude_name")
    db_latitude_UID = cfg.get("Database", "db_latitude_UID")
    db_latitude_pwd = cfg.get("Database", "db_latitude_pwd")
    db_commerce_center_ip = cfg.get("Database", "db_commerce_center_ip")
    db_commerce_center_name = cfg.get("Database", "db_commerce_center_name")
    db_commerce_center_UID = cfg.get("Database", "db_commerce_center_UID")
    db_commerce_center_pwd = cfg.get("Database", "db_commerce_center_pwd")
    ge_account_number = cfg.get("CompanyInfo", "ge_account_number")
    output_file_name = cfg.get("OutputPaths", "output_file_base") + "-" + query_date + ".txt"
    company_name = cfg.get("CompanyInfo", "company_name")
    cust_id = cfg.get("CompanyInfo", "cust_id")
    archive_path = cfg.get("OutputPaths", "archive_path")
    ftp_log_path = cfg.get("OutputPaths", "ftp_log_path")
    temp_path = cfg.get("OutputPaths", "temp_path")
    ftp_username = cfg.get("FTP", "ftp_username")
    ftp_password = cfg.get("FTP", "ftp_password")
    ftp_url = cfg.get("FTP", "ftp_url")
    excluded_product_groups = cfg.get("Exclusions", "product_groups").split(" ")
    win_scp_path = cfg.get("General", "win_scp_path")
except:
    print "\n==========| Exception raised!!!! |=========="
    print "\tThere was an error parsing the configuration file."
    print "\tThis normally occurs when a section is missing"
    print "\tPlease ensure all sections and options are present, options may be left blank.  For example:"
    print "\t\t[Exclusions]"
    print "\t\tproduct_groups : "
    print "\n\tPlease see the README file for further details."
    print "==========| End exception report |=========="
    exit(1)

#==========| Configuration strings |==========

if DEBUG:
    print cfg.sections()
    print "Latitude DB server IP:\t\t\t", db_latitude_ip
    print "Latitude DB name:\t\t\t", db_latitude_name
    print "Latitude DB UID:\t\t\t", db_latitude_UID
    print "Latitude DB pwd:\t\t\t", db_latitude_pwd
    print "P21 DB server IP:\t\t\t", db_commerce_center_ip
    print "Commerce Center DB name:\t", db_commerce_center_name
    print "Commerce Center DB UID:\t\t", db_commerce_center_UID
    print "Commerce Center DB pwd:\t\t", db_commerce_center_pwd
    print "GE Account number:\t\t\t", ge_account_number
    print "Output file name:\t\t\t", output_file_name
    print "Company name:\t\t\t\t", company_name
    print "GE Customer ID:\t\t\t\t", cust_id
    print "Archive Path:\t\t\t\t", archive_path
    print "FTP Log Path:\t\t\t\t", ftp_log_path
    print "Temp file path:\t\t\t\t", temp_path
    print "Excluded product groups:\t", excluded_product_groups
    print "WinSCP Path:\t\t\t\t", win_scp_path

print "Running queries...."

# exit(1)
# QUERY 1: All sales that are shipped through Latitute
#   Query includes Latitude database to ensure that if the Latitiude Gateway goes down we don't miss
#     information not yet propagated to CommerceCenter
#    * 07-22-2015 - Join on item_id instead of line_number to avoid P21/Latitude item mismatches
#    * 09-22-2015 - Added exclusion to where clause for items with zero-dollar price

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db_latitude_ip + ';DATABASE=' + db_latitude_name + ';UID=' +
                      db_latitude_UID + ';PWD=' + db_latitude_pwd)
query1 = '''
    SELECT DISTINCT
      NULL as is_rma,
      appr.floor_plan_approval_number AS 'Appr_No',
      ihead.carrier_name,
      CONVERT(varchar,ihead.invoice_date,112) AS 'Inv_Date',
      CONVERT(varchar,ihead.net_due_date,112) AS 'Due_Date',
      ihead.invoice_no,
      ihead.order_no,
      ihead.po_no AS Po_num,
      ihead.ship2_address1 as ship_addr1,
      ihead.ship2_address2 AS ship_addr2,
      ihead.ship2_city AS ship_city,
      ihead.ship2_state AS ship_state,
      ihead.ship2_postal_code AS ship_zip,
      ihead.total_amount AS 'Inv_Total',
      ihead.terms_id AS Terms,
      iline.unit_price AS Price,
      iline.product_group_id AS Prod,
      iline.item_desc,
      mail_to.name mail_name,
      mail_to.mail_address1 mail_addr1,
      mail_to.mail_address2 mail_addr2,
      mail_to.mail_city mail_city,
      mail_to.mail_state mail_state,
      mail_to.mail_postal_code mail_zip,
      oept.pick_ticket_no,
      trueterms.floor_plan_id,
      trueterms.floor_plan_desc,
      SO.CustomerID AS Dealer,
      SO.BillToName as bill_to,
      SOS.TrackingNumber 'tracking_no',
      SOL.ItemID,
      SOL.PickQuantity AS Qty,
      SOL.LineNumber AS Ln,
      COALESCE(SN.SerialNumberID,'') AS SerialNumberID

    FROM SalesOrder SO
    INNER JOIN SalesOrderLine SOL ON SOL.SalesOrderID = SO.SalesOrderID
    LEFT JOIN SalesOrderSerialNumber SN ON SN.SalesOrderID = SOL.SalesOrderID AND SN.LineNumber = SOL.LineNumber
    INNER JOIN SalesOrderShip SOS on SOS.SalesOrderID = SO.SalesOrderId AND SOS.BoxNumber = '1'
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.invoice_hdr ihead ON ihead.invoice_no = SO.InvoiceNumber
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.invoice_line iline ON iline.invoice_no = ihead.invoice_no AND iline.item_id = SOL.itemID --iline.line_no = SOL.LineNumber
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.oe_hdr oehdr ON oehdr.order_no = ihead.order_no
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.order_floor_plan_xref_10002 appr ON appr.oe_hdr_uid = oehdr.oe_hdr_uid
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.floor_plan_10002 trueterms ON appr.floor_plan_uid = trueterms.floor_plan_uid
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.address mail_to ON ihead.sold_to_customer_id = mail_to.id
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.oe_pick_ticket oept ON oept.invoice_no = ihead.invoice_no '''
if len(excluded_product_groups) > 0:
    query1 += '''
    inner join ''' + db_commerce_center_name + '''.dbo.inv_mast invm on invm.inv_mast_uid = iline.inv_mast_uid
    inner join ''' + db_commerce_center_name + '''.dbo.inv_loc invl on invl.inv_mast_uid = invm.inv_mast_uid
    '''
query1 += '''
                WHERE
                  ihead.customer_id = ''' + "'" + ge_account_number + "'" + '''
                  AND SO.DocumentType = 'SO'
                  AND iline.inv_mast_uid IS NOT NULL
                  AND iline.unit_price <> 0
                  AND CONVERT(varchar,ihead.invoice_date,112) = ''' + "'" + query_date + "'" + '''
                  AND CONVERT(varchar,oept.ship_date,112) = ''' + "'" + query_date + "'"
if len(excluded_product_groups) > 0:
    query1 += " AND invl.product_group_id not in (" + str(excluded_product_groups).strip('[]') + ")"
query1 += '''
                  ORDER BY ihead.invoice_no asc  '''

cursor = cnxn.cursor()
cursor.execute(query1)

# QUERY 2: All sales that are NOT recorded in Latitude
#   Query includes other sales such as Drop Ships and Front Counter sales
#    * 11-19-2014:  Removed oept.ship_date from query where clause as direct ships can have different ship_to and
#                       invoice dates
#    * 09-22-2015 - Added exclusion to where clause for items with zero-dollar price

cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db_latitude_ip + ';DATABASE=' + db_latitude_name + ';UID=' +
                      db_latitude_UID + ';PWD=' + db_latitude_pwd)
query2 = '''
    SELECT DISTINCT
      NULL as is_rma,
      'Transfer_or_DirectShip' as 'tracking_no',
      ihead.invoice_no,
      ihead.order_no,
      oept.pick_ticket_no,
      LTRIM(RTRIM(dls.serial_number)) as SerialNumberID,
      dls.document_line_serial_uid,
      dls.row_status,
      ihead.carrier_name,
      appr.floor_plan_approval_number AS 'Appr_No',
      ihead.total_amount AS 'Inv_Total',
      oehdr.customer_id AS Dealer,
      CONVERT(varchar,ihead.invoice_date,112) AS 'Inv_Date',
      CONVERT(varchar,ihead.net_due_date,112) AS 'Due_Date',
      ihead.terms_id AS Terms,
      trueterms.floor_plan_id,
      trueterms.floor_plan_desc,
      ihead.po_no AS Po_num,
      iline.product_group_id AS Prod,
      iline.item_desc,
      iline.item_id ItemID,
      iline.qty_shipped Qty,
      iline.unit_price AS Price,
      oehdr.ship2_name bill_to,
      ihead.ship2_address1 as ship_addr1,
      ihead.ship2_address2 AS ship_addr2,
      ihead.ship2_city AS ship_city,
      ihead.ship2_state AS ship_state,
      ihead.ship2_postal_code AS ship_zip,
      mail_to.name mail_name,
      mail_to.mail_address1 mail_addr1,
      mail_to.mail_address2 mail_addr2,
      mail_to.mail_city mail_city,
      mail_to.mail_state mail_state,
      mail_to.mail_postal_code mail_zip

    FROM ''' + db_commerce_center_name + '''.dbo.invoice_hdr ihead
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.invoice_line iline ON iline.invoice_no = ihead.invoice_no
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.oe_hdr oehdr ON oehdr.order_no = ihead.order_no
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.order_floor_plan_xref_10002 appr ON appr.oe_hdr_uid = oehdr.oe_hdr_uid
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.floor_plan_10002 trueterms ON appr.floor_plan_uid = trueterms.floor_plan_uid
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.address mail_to ON ihead.sold_to_customer_id = mail_to.id
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.oe_pick_ticket oept ON oept.order_no = ihead.order_no
    INNER JOIN ''' + db_commerce_center_name + '''.dbo.oe_pick_ticket_detail oeptd ON oept.pick_ticket_no = oeptd.pick_ticket_no
        and CONVERT(varchar,oeptd.date_created,112) >= ''' + "'" + query_date + "'" + '''
    inner join ''' + db_commerce_center_name + '''.dbo.document_line_serial dls on dls.document_no = oept.pick_ticket_no
        and dls.line_no = iline.line_no '''
if len(excluded_product_groups) > 0:
    query2 += '''
    inner join ''' + db_commerce_center_name + '''.dbo.inv_mast invm on invm.inv_mast_uid = iline.inv_mast_uid
    inner join ''' + db_commerce_center_name + '''.dbo.inv_loc invl on invl.inv_mast_uid = invm.inv_mast_uid
    '''
query2 += '''
    WHERE
      ihead.customer_id = ''' + "'" + ge_account_number + "'" + '''
      AND iline.qty_shipped > 0
      AND iline.inv_mast_uid IS NOT NULL
      AND iline.unit_price <> 0
      AND CONVERT(varchar,ihead.invoice_date,112) = ''' + "'" + query_date + "'" + '''
      AND ihead.invoice_no NOT IN (
            select InvoiceNumber from SalesOrder SO
                INNER JOIN ''' + db_commerce_center_name + '''.dbo.invoice_hdr ihead ON ihead.invoice_no = SO.InvoiceNumber
                INNER JOIN SalesOrderLine SOL on SO.SalesOrderId = SOL.SalesOrderID
                INNER JOIN ''' + db_commerce_center_name + '''.dbo.oe_pick_ticket oept ON oept.order_no = ihead.order_no
            where CONVERT(varchar,ihead.invoice_date,112) =  ''' + "'" + query_date + "'" + '''
                AND CONVERT(varchar,oept.ship_date,112) =  ''' + "'" + query_date + "'" + '''
                AND ihead.customer_id = ''' + "'" + ge_account_number + "'" + '''
                AND (SOL.LineStatus != '' AND SO.OrderStatus = 'T')
                )
  '''
if len(excluded_product_groups) > 0:
    query2 += " AND invl.product_group_id not in (" + str(excluded_product_groups).strip('[]') + ")"
query2 += '''
                  ORDER BY ihead.invoice_no asc
                    '''

cursor_non_latitude = cnxn2.cursor()
cursor_non_latitude.execute(query2)

# QUERY 3: RMA returns
#   Query includes all SERIALIZED RMA returns that need to be credited
#    * 09-22-2015 - Added exclusion to where clause for items with zero-dollar price

cnxn3 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db_commerce_center_ip + ';DATABASE=' + db_commerce_center_name +
                       ';UID=' + db_commerce_center_UID + ';PWD=' + db_commerce_center_pwd)
query3 = '''
            select distinct oehdr.customer_id 'Dealer',
                        '1' as is_rma,
                        LTRIM(RTRIM(dls.serial_number)) as SerialNumberID,
                        dls.document_line_serial_uid,
                        dls.row_status,
                        ihead.order_no,
                        iline.item_id 'ItemID',
                        iline.qty_shipped Qty,
                        ihead.invoice_no,
                        ihead.total_amount AS 'Inv_Total',
                        trueterms.floor_plan_id,
                        ihead.carrier_name,
                        oept.pick_ticket_no,
                        iline.product_group_id AS Prod,
                        iline.item_desc,
                        iline.unit_price AS Price,
                        ihead.po_no AS Po_num,
                        '' as tracking_no,
                        CONVERT(varchar,ihead.invoice_date,112) AS 'Inv_Date',
                        oehdr.ship2_name 'bill_to',
                        oehdr.ship2_add1 'ship_addr1',
                        oehdr.ship2_add2 'ship_addr2',
                        oehdr.ship2_city 'ship_city',
                        oehdr.ship2_state 'ship_state',
                        oehdr.ship2_zip 'ship_zip',
                        mail_to.name mail_name,
                        mail_to.mail_address1 mail_addr1,
                        mail_to.mail_address2 mail_addr2,
                        mail_to.mail_city mail_city,
                        mail_to.mail_state mail_state,
                        mail_to.mail_postal_code mail_zip,
                        appr.floor_plan_approval_number AS 'Appr_No'

            from rma_receipt_hdr rma_hdr
                inner join rma_receipt_line rma_line on rma_hdr.rma_receipt_hdr_uid = rma_line.rma_receipt_hdr_uid
                inner join oe_hdr oehdr on oehdr.oe_hdr_uid = rma_hdr.oe_hdr_uid
                inner join document_line_serial dls on dls.document_no = rma_hdr.rma_receipt_hdr_uid
                    and dls.line_no = rma_line.receipt_line_no
                inner join invoice_hdr ihead on oehdr.order_no = ihead.order_no
                INNER JOIN invoice_line iline ON iline.invoice_no = ihead.invoice_no
                    and dls.line_no = iline.line_no
                INNER JOIN address mail_to ON ihead.ship_to_id = mail_to.id
                INNER JOIN order_floor_plan_xref_10002 appr ON appr.oe_hdr_uid = oehdr.oe_hdr_uid
                INNER JOIN floor_plan_10002 trueterms ON appr.floor_plan_uid = trueterms.floor_plan_uid
                INNER JOIN oe_pick_ticket oept ON oept.order_no = ihead.order_no'''
if len(excluded_product_groups) > 0:
    query3 += '''
    inner join inv_mast invm on invm.inv_mast_uid = iline.inv_mast_uid
    inner join inv_loc invl on invl.inv_mast_uid = invm.inv_mast_uid
    '''
query3 += '''
            where CONVERT(varchar,rma_hdr.date_created,112) = ''' + "'" + query_date + "'" + '''
                AND iline.unit_price <> 0
                AND ihead.customer_id = ''' + "'" + ge_account_number + "'"
if len(excluded_product_groups) > 0:
    query3 += " AND invl.product_group_id not in (" + str(excluded_product_groups).strip('[]') + ")"

cursor_rma = cnxn3.cursor()
cursor_rma.execute(query3)


# QUERY 4: RMA returns with no serial numbers
#   Query includes all NON-SERIALIZED RMA returns that need to be credited
#    * 09-22-2015 - Added exclusion to where clause for items with zero-dollar price

cnxn4 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db_commerce_center_ip + ';DATABASE=' + db_commerce_center_name +
                       ';UID=' + db_commerce_center_UID + ';PWD=' + db_commerce_center_pwd)
query4 = '''
    with x as (
    select rma_hdr.rma_receipt_hdr_uid,
            rma_hdr.oe_hdr_uid,
            rma_hdr.receipt_no,
            rma_line.rma_receipt_line_uid,
            rma_line.oe_line_uid,
            rma_line.qty_received,
            dls.serial_number,
            oehdr.order_no

    from rma_receipt_hdr rma_hdr
        inner join rma_receipt_line rma_line on rma_hdr.rma_receipt_hdr_uid = rma_line.rma_receipt_hdr_uid
        inner join oe_hdr oehdr on oehdr.oe_hdr_uid = rma_hdr.oe_hdr_uid
        inner join invoice_hdr ihead on oehdr.order_no = ihead.order_no
        left outer join document_line_serial dls on dls.document_no = rma_hdr.rma_receipt_hdr_uid
            and dls.line_no = rma_line.receipt_line_no
    where CONVERT(varchar,rma_hdr.date_created,112) = ''' + "'" + query_date + "'" + '''
        AND ihead.customer_id = ''' + "'" + ge_account_number + "'" + '''
)
select DISTINCT x.*,
        oehdr.customer_id 'Dealer',
        '1' as is_rma,
        'Null-Serial' as SerialNumberID,
        ihead.order_no,
        ihead.invoice_no,
        ihead.total_amount AS 'Inv_Total',
        ihead.carrier_name,
        ihead.po_no AS Po_num,
        '' as tracking_no,
        CONVERT(varchar,ihead.invoice_date,112) AS 'Inv_Date',
        iline.item_id 'ItemID',
        iline.qty_shipped Qty,
        iline.product_group_id AS Prod,
        iline.item_desc,
        iline.unit_price AS Price,
        oept.pick_ticket_no,
        oehdr.ship2_name 'bill_to',
        oehdr.ship2_add1 'ship_addr1',
        oehdr.ship2_add2 'ship_addr2',
        oehdr.ship2_city 'ship_city',
        oehdr.ship2_state 'ship_state',
        oehdr.ship2_zip 'ship_zip',
        mail_to.name mail_name,
        mail_to.mail_address1 mail_addr1,
        mail_to.mail_address2 mail_addr2,
        mail_to.mail_city mail_city,
        mail_to.mail_state mail_state,
        mail_to.mail_postal_code mail_zip,
        trueterms.floor_plan_id,
        appr.floor_plan_approval_number AS 'Appr_No'
from x
    inner join oe_line oeline on oeline.oe_line_uid = x.oe_line_uid
    inner join oe_hdr oehdr on oehdr.oe_hdr_uid = oeline.oe_hdr_uid
    inner join invoice_hdr ihead on ihead.order_no = oehdr.order_no
    inner join invoice_line iline on iline.invoice_no = ihead.invoice_no
        and iline.oe_line_number = oeline.line_no
    inner join oe_pick_ticket oept on oept.order_no = ihead.order_no
    inner join address mail_to on mail_to.id = ihead.ship_to_id
    inner join order_floor_plan_xref_10002 appr on appr.oe_hdr_uid = oehdr.oe_hdr_uid
    inner join floor_plan_10002 trueterms on trueterms.floor_plan_uid = appr.floor_plan_uid '''
if len(excluded_product_groups) > 0:
    query4 += '''
    inner join inv_mast invm on invm.inv_mast_uid = iline.inv_mast_uid
    inner join inv_loc invl on invl.inv_mast_uid = invm.inv_mast_uid
    '''
query4 += '''
where x.serial_number is null
    AND iline.unit_price <> 0
    '''
if len(excluded_product_groups) > 0:
    query4 += " AND invl.product_group_id not in (" + str(excluded_product_groups).strip('[]') + ")"

cursor_rma_non_serial = cnxn4.cursor()
cursor_rma_non_serial.execute(query4)

if DEBUG:
    print "====================| Query 1 |===================="
    print query1
    print "====================| Query 2 |===================="
    print query2
    print "====================| Query 3 |===================="
    print query3
    print "====================| Query 4 |===================="
    print query4

print "Queries complete, parsing and exporting data...."

# BATCH HEADER DEFS
batch_number = query_date.rjust(10, '0')
date_time = (time.strftime("%m%d%y")) + (time.strftime("%H%M%S"))

# TODO:  Since we can now recreate old dates, we should probably handle the collisions
output_file = open(output_file_name, "w")

# Function Definitions

def invoice_header(row, other_charges):

    if DEBUG and row.invoice_no in other_charges:
        print row.invoice_no
        print "%.2f" % (row.Inv_Total - other_charges[row.invoice_no])

    IH_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "1".ljust(2) + str(row.Inv_Date)[4:] + \
        str(row.Inv_Date)[2:4] + str(row.Appr_No)[:6].ljust(6)
    if row.invoice_no in other_charges:
        IH_Line += str("%.2f" % (row.Inv_Total - other_charges[row.invoice_no])).translate(None, '.-').rjust(11, '0')
    else:
        IH_Line += str(row.Inv_Total)[:-2].translate(None, '.-').rjust(11, '0')
    if row.is_rma is not None:
        IH_Line += '-'
    else:
        IH_Line += ' '
    IH_Line += row.floor_plan_id[:5].ljust(5) + " ".ljust(16) + row.Po_num.ljust(20) + \
        " ".ljust(10) + "0".ljust(11, '0') + " ".ljust(9)
    output_file.write(IH_Line + '\n')
    if DEBUG == 1:
        print IH_Line

def address_info(row):
    # Name Record
    SLDN_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "6SLDN" + str(row.bill_to)[:35].ljust(35) + \
        ' '.ljust(35) + "USA".ljust(22)
    output_file.write(SLDN_Line + '\n')

    # Address Record (Bill-to Address)
    SLDA_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "6SLDA"
    if row.mail_addr2 is not None:
        SLDA_Line += str(row.mail_addr1).ljust(35) + str(row.mail_addr2).ljust(57)
    else:
        SLDA_Line += str(row.mail_addr1).ljust(92)
    output_file.write(SLDA_Line + '\n')

    # City - County - State - ZIP Record
    SLDC_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "6SLDC" + str(row.mail_city).ljust(50) + \
        str(row.mail_state).ljust(25) + str(row.mail_zip).ljust(17)
    output_file.write(SLDC_Line + '\n')

    # Name Record
    SHPN_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "6SHPN" + str(row.mail_name)[:35].ljust(
        35) + ' '.ljust(35) + "USA".ljust(22)
    output_file.write(SHPN_Line + '\n')

    # Address Record (Ship-to address)
    SHPA = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "6SHPA"
    if row.ship_addr2 is not None:
        SHPA = SHPA + str(row.ship_addr1).ljust(35) + str(row.ship_addr2).ljust(57)
    else:
        SHPA += str(row.ship_addr1).ljust(92)
    output_file.write(SHPA + '\n')

    # City - County - State - ZIP Record
    SHPC_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "6SHPC" + str(row.ship_city).ljust(50) + \
        str(row.ship_state).ljust(25) + str(row.ship_zip).ljust(17)
    output_file.write(SHPC_Line + '\n')

    # Additional Header AH03
    AH03_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "HAH03" + str(row.carrier_name).ljust(35) + \
        str(row.tracking_no).ljust(57)
    output_file.write(AH03_Line + '\n')

    # Additional Header AH04
    # First build the SO and PT string so we can do the justification
    SOPT = "SO " + str(row.order_no) + " PT " + str(row.pick_ticket_no)
    AH04_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "HAH04" + " ".ljust(30) + SOPT.ljust(62)
    output_file.write(AH04_Line + '\n')

    if DEBUG == 1:
        print SLDN_Line
        print SLDA_Line
        print SLDC_Line
        print SHPN_Line
        print SHPA
        print SHPC_Line
        print AH03_Line
        print AH04_Line

def item_detail(row):
    # Return row total for file checksum
    # Invoice Detail Record

    ID1_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "2".ljust(2)
    # Test for NULL serial numbers
    if row.SerialNumberID == None or row.SerialNumberID == "":
        ID1_Line += "NULL-Serial".ljust(20)
    else:
        ID1_Line += str(row.SerialNumberID).ljust(20)
        # Is this a RMA?
    if row.is_rma != None:
        ID1_Line += str(row.Price)[:(str(row.Price)).index('.') + 3].translate(None, '.').rjust(11, '0') + '-' + \
            str(row.ItemID).split(' ')[0].ljust(4) + str(row.ItemID).split(' ')[1].ljust(60)
    else:
        ID1_Line += str(row.Price)[:(str(row.Price)).index('.') + 3].translate(None, '.').rjust(11, '0') + ' ' + \
            str(row.ItemID).split(' ')[0].ljust(4) + str(row.ItemID).split(' ')[1].ljust(59)
    output_file.write(ID1_Line + '\n')

    # Additional Detail AD02
    AD02_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "DAD02" + str(row.item_desc).ljust(92)
    output_file.write(AD02_Line + '\n')

    # Additional Detail AD08
    AD08_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "DAD08" + str(row.ItemID).ljust(92)
    output_file.write(AD08_Line + '\n')

    if DEBUG == 1:
        print ID1_Line
        print AD02_Line
        print AD08_Line

    return row.Price

# This function finds charges applied to the invoice as 'other charges', which show up in the total of the invoice
#   however are actually additions/subtractions to the subtotal and NOT line items.  These totals need to be
#   subtracted from the stored invoice_amount in order to have the line item totals and the invoice total to match.
#
def get_charges_to_exclude():

    cnxn5 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db_commerce_center_ip + ';DATABASE=' + db_commerce_center_name +
                           ';UID=' + db_commerce_center_UID + ';PWD=' + db_commerce_center_pwd)
    cnxn6 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db_commerce_center_ip + ';DATABASE=' + db_commerce_center_name +
                           ';UID=' + db_commerce_center_UID + ';PWD=' + db_commerce_center_pwd)

    # Query to exclude any items that are of type 'other charge' on the invoice
    query = '''
           select iline.invoice_no,
                iline.item_id,
                iline.unit_price,
                iline.qty_shipped,
                ihead.total_amount
        from
                invoice_line as iline
                INNER JOIN invoice_hdr as ihead
                    on ihead.invoice_no = iline.invoice_no
            WHERE CONVERT(varchar,ihead.invoice_date,112) = ''' + "'" + query_date + "'" + '''
                 AND ihead.customer_id = ''' + "'" + ge_account_number + "'" + '''
                 AND iline.other_charge_item = 'Y'
        '''

    query2 = '''
            select ihead.invoice_no,
                    ihead.total_amount,
                    CONVERT(varchar,ihead.invoice_date,112) 'Invoice Date',
                    oept.freight_out
                from oe_pick_ticket oept
                INNER JOIN invoice_hdr ihead
                    ON oept.invoice_no = ihead.invoice_no
                INNER JOIN freight_code as fc
                    ON fc.freight_code_uid = oept.freight_code_uid
                WHERE ihead.customer_id = ''' + "'" + ge_account_number + "'" + '''
                AND CONVERT(varchar,ihead.invoice_date,112) = ''' + "'" + query_date + "'" + '''
                AND fc.outgoing_freight = 'Y'
                AND oept.freight_out > 0
        '''

    # cursor has other_charge items
    cursor = cnxn5.cursor()
    cursor2 = cnxn6.cursor()

    # cursor2 handles latitude PPDADD freight
    #  or any freight that is a charge on the invoice (check freight_code table)
    cursor.execute(query)
    cursor2.execute(query2)
    other_charge_sum = {}

    # Since we are using a dictionary, we must either add a new key or lookup the key and add to the invoice sum

    if cursor.rowcount < 0:
        print " ==> Adjusting for other_charge items detected on invoices"
        for row in cursor:
            if row.invoice_no in other_charge_sum:
                other_charge_sum[row.invoice_no] += (row.unit_price * row.qty_shipped)
            else:
                other_charge_sum[row.invoice_no] = (row.unit_price * row.qty_shipped)
    else:
        print " --> No other_charge items affect subtotal"

    # Do the same for PPDADD freight
    if cursor2.rowcount < 0:
        print " ==> Adjusting for PPDADD freight (or any other freight) that was detected on invoices"
        for row in cursor2:
            if row.invoice_no in other_charge_sum:
                other_charge_sum[row.invoice_no] += row.freight_out
            else:
                other_charge_sum[row.invoice_no] = row.freight_out
    else:
        print " --> No Latitude shipping charges affect subtotal"

    # Return the dictionary to the main program
    return other_charge_sum

# Main Program
# Print BATCH HEADER RECORD

# The zero signifies the record as a batch header
str_batch_header = company_name.ljust(23) + "0".ljust(30) + cust_id + batch_number.ljust(11) + date_time.ljust(52)
output_file.write(str_batch_header + '\n')

# put queried data into lists for parsing
rows = cursor.fetchall()
rma_rows = cursor_rma.fetchall()
non_latitude_rows = cursor_non_latitude.fetchall()
rma_non_serial = cursor_rma_non_serial.fetchall()
other_charges = get_charges_to_exclude()

if DEBUG >= 1:
    print str_batch_header
    print "RMA cursor length ====>                " + str(len(rma_rows))
    print "RMA without serial cursor length ====> " + str(len(rma_non_serial))
    print "Sales cursor length ==>                " + str(len(rows))
    print "All Invoice cursor length ===>         " + str(len(non_latitude_rows))
    print "Invoices with other charges ===>       " + str(len(other_charges.keys()))

# Set counter/flag variables
current = None              # current invoice flag
header_count = 0            # count for number of header records
batch_total = 0             # accumulator for total of all sales
debug_item_count = 0        # debug variable for item counting
invoice_total = 0           # debug variable for totaling invoice amounts
num_to_print = 1            # flag/counter for quantity printing

# Latitude-shipped sales
for row in rows:
    # If the item is serialized, we only print one, otherwise we have to respect the quantity
    if len(row.SerialNumberID) > 1:
        num_to_print = 1
    else:
        num_to_print = row.Qty
    while num_to_print > 0:
        # Check to see if we need a new invoice header record or if it is the same invoice
        if current != row.invoice_no:
            header_count += 1
            invoice_total += row.Inv_Total
            current = row.invoice_no
            invoice_header(row, other_charges)
            address_info(row)
            batch_total = batch_total + item_detail(row)
        else:
            batch_total = batch_total + item_detail(row)
        num_to_print -= 1
        if DEBUG == 10:
            print "item price:", str(row.Price), "\tQuantity:", str(row.Qty), "\t\trunning batch total: ", str(batch_total), \
                row.ItemID, row.item_desc


# Non-Latitude sales
#  - Since the query does not pull one line per item, we use the quantity field when printing
if len(non_latitude_rows) > 0:
    for row in non_latitude_rows:
        # Check to see if we need a new invoice header record or if it is the same invoice
        if current != row.invoice_no:
            header_count += 1
            invoice_total += row.Inv_Total
            current = row.invoice_no
            invoice_header(row, other_charges)
            address_info(row)
            batch_total = batch_total + item_detail(row)
        else:
            batch_total = batch_total + item_detail(row)
        if DEBUG == 10:
            print "item price:", str(row.Price), "\tQuantity:", str(row.Qty), "\t\trunning batch total: ", str(batch_total), \
                row.ItemID, row.item_desc

# RMA Loop.  Since these are all returns we can switch to pure subtraction
if len(rma_rows) > 0:
    for row in rma_rows:
        # Check to see if we need a new invoice header record or if it is the same invoice
        if current != row.invoice_no:
            header_count += 1
            invoice_total += row.Inv_Total
            current = row.invoice_no
            invoice_header(row, other_charges)
            address_info(row)
            batch_total = batch_total - item_detail(row)
        else:
            batch_total = batch_total - item_detail(row)
        if DEBUG == 10:
            print "item price:", str(row.Price), "\tQuantity:", str(row.Qty), "\t\trunning batch total: ", \
                str(batch_total), row.ItemID, row.item_desc

# RMA non-serialized
#  - Since the query does not pull one line per item, we use the quantity field when printing

if len(rma_non_serial) > 0:
    for row in rma_non_serial:
        num_to_print = abs(row.Qty)
        while num_to_print > 0:
            # Check to see if we need a new invoice header record or if it is the same invoice
            if current != row.invoice_no:
                header_count += 1
                invoice_total += row.Inv_Total
                current = row.invoice_no
                invoice_header(row, other_charges)
                address_info(row)
                batch_total = batch_total - item_detail(row)
            else:
                batch_total = batch_total - item_detail(row)
            num_to_print -= 1
            if DEBUG == 10:
                print "item price:", str(row.Price), "\tQuantity:", str(row.Qty), "\t\trunning batch total: ", \
                    str(batch_total), row.ItemID, row.item_desc

# Check to see if we have any activity to print, if not then we can log this and avoid a file transfer
if len(rows) == 0 and len(rma_rows) == 0 and len(non_latitude_rows) == 0 and len(rma_non_serial) == 0:
    activity = False
else:
    activity = True

# Batch Trailer Record
#
# It is possible we will have no activity to report, if so we note the fact and just exit
if not activity:
    print "  ==== NO ACTIVITY FOR " + time.strftime("%Y%m%d") + " ===="
    output_file.write("  ==== NO ACTIVITY FOR " + query_date + " ====" + '\n')
    output_file.close()
    print "No activity in file, skipping file transfer...."
    print "Archiving activity file..."
    shutil.copy2(output_file_name, archive_path)
    os.remove(output_file_name)
    print "Archive complete....exiting."
    exit(0)
else:
    # Print the file footer record
    file_footer = company_name.ljust(23) + "9".ljust(7) + str(header_count).rjust(5, '0') + \
        str(batch_total)[:(str(batch_total)).index('.') + 3].translate(None, '.-').rjust(13, '0')
    # Figure out whether we have a negative or positive batch total
    if str(batch_total).count('-') > 0:
        file_footer += '-'
    else:
        file_footer += ' '
    file_footer += cust_id.rjust(8) + date_time.rjust(23).ljust(63)
    output_file.write(file_footer + '\n')
    if DEBUG == 1:
        print file_footer
    if DEBUG > 0:
        print "Invoice Totals: ", invoice_total, "\t\tItem Totals: ", batch_total

# Close the file so we can send it....
output_file.close()
print "Export complete...."

# Put the file in the temporary FTP location
shutil.copy2(output_file_name, temp_path)

# Replace forward slashes with back slashes for command line commands
print "Starting file transfer...."
transfer_cmd = win_scp_path + ' /script=WinSCP-commands.cmd ' + \
    ' /xmllog=' + ftp_log_path.replace('/', '\\') + '\!Y-!M-!D-!T-log.xml /log=' + ftp_log_path.replace('/', '\\') + \
    '\!Y-!M-!D-!T-log.txt' + ' /parameter ' + output_file_name + ' ' + temp_path + ' ' + ftp_username + ' ' + \
    ftp_password + ' ' + ftp_url

if subprocess.call(transfer_cmd):
    print "An error occurred during FTP transfer.  This was logged in " + ftp_log_path
    exit(-1)
else:
    print "Transfer successful...archiving activity file...."
    shutil.copy2(output_file_name, archive_path)
    os.remove(temp_path + '/' + output_file_name)
    os.remove(output_file_name)
    print "Archive complete....exiting."
    exit(0)

# pysftp code.  Unfortunately this does not work as it should.
# Sending via sFTP fails on remote file open request, tabling this for now in favor of another solution
# import pysftp    # << need this import for the rest to work
# print "Starting file transfer...."
# sftp_cnxn = 'host="' + ftp_url + '", username="' + ftp_username + '", password="' + ftp_password + '"'
# dest = pysftp.Connection(sftp_cnxn)
# print "Dest cwd:  " + dest.pwd
# print "Attempting to put " + output_file_name
# dest.put(output_file_name)    # <<<<< This fails with insufficient permissions
# for i in dest.listdir():
#     print i
