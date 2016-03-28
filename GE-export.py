import sys
import pyodbc
import time
import subprocess
import shutil
import os
import ConfigParser

# Set to 1 or 10 to get extra output to the console
DEBUG = 0
ADJUST_LOG = ""
EXCLUSION_LOG = ""

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
    email_server_ip = cfg.get("Email", "email_server_ip")
    email_from_address = cfg.get("Email", "email_from_address")
    email_to_addresses = cfg.get("Email", "email_to_addresses").split(" ")

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
      ihead.ship2_name mail_name,
      --mail_to.name mail_name,
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
      COALESCE(SN.SerialNumberID, NULL) AS SerialNumberID

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
#    * 09-25-2015 - Adjusted document_line_serial join to account for re-ordered line numbers during substitutions
#    * 01-14-2016 - Adjust query to handle edge case of invoice with no serialized items
#    * 02-09-2016 - Adjust query to screen off less information (comment out lines in the query from temp table)
#    * 02-19-2016 - Fix adjustment made on 02-09 to the WHERE clause for the temporary table, there are
#                     two cases, both listed below:
#                   -> Clause part 1 deals with serialized items
#                   -> Clause part 2 deals with non-serialzed items, quantity must be respected for these lines
#                   Some Data Definitions
#                     -> line_no        -- document_line_serial.line_no
#                     -> line_number    -- oe_pick_ticket_detail
#                     -> oe_line_no     -- oe_pick_ticket_detail.oe_line_no (matches invoice_line.oe_line_number)
#                     -> oe_line_number -- invoice_line.oe_line_number, matches above
#
#                   So, in this case, we have part 1, there will always be a document_no and a serial no.
#                   For part 2, there will never be a document no and the last 3 numbers should match to
#                     avoid duplicates creeping in.


cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db_latitude_ip + ';DATABASE=' + db_latitude_name + ';UID=' +
                      db_latitude_UID + ';PWD=' + db_latitude_pwd)
query2 = '''
  with x as (

    SELECT DISTINCT
      NULL as is_rma,
      'Transfer_or_DirectShip' as 'tracking_no',
      ihead.invoice_no,
      ihead.order_no,
      oept.pick_ticket_no,
      dls.line_no,
      oeptd.line_number,
      oeptd.oe_line_no,
      iline.oe_line_number,
      dls.document_no,
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
      ihead.ship2_name mail_name,
      --mail_to.name mail_name,
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
    left join ''' + db_commerce_center_name + '''.dbo.document_line_serial dls on dls.document_no = oept.pick_ticket_no
        and dls.line_no = oeptd.line_number and oeptd.oe_line_no = iline.oe_line_number '''
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
  )
  select * from x
    where (
        x.line_no = x.line_number
--        and x.line_no = x.oe_line_no
--        and x.line_no = x.oe_line_number
        ) OR
        (
        x.document_no is NULL
        and x.line_number = x.oe_line_no
        and x.line_number = x.oe_line_number
        )
--        AND x.line_no IS NOT NULL
      ORDER BY x.invoice_no asc
        '''

cursor_non_latitude = cnxn2.cursor()
cursor_non_latitude.execute(query2)

# QUERY 3: RMA returns
#   Query includes all SERIALIZED RMA returns that need to be credited
#    * 09-22-2015 - Added exclusion to where clause for items with zero-dollar price
#    * 10-05-2015 - Added missing 'order by' clause.  This prevents multiple headers from appearing per invoice

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
                        ihead.ship2_name mail_name,
                        --mail_to.name mail_name,
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
query3 += '''
            order by ihead.invoice_no asc'''

cursor_rma = cnxn3.cursor()
cursor_rma.execute(query3)


# QUERY 4: RMA returns with no serial numbers
#   Query includes all NON-SERIALIZED RMA returns that need to be credited
#    * 09-22-2015 - Added exclusion to where clause for items with zero-dollar price
#    * 10-05-2015 - Added missing 'order by' clause.  This prevents multiple headers from appearing per invoice

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
        ihead.ship2_name mail_name,
      --mail_to.name mail_name,
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
    query4 += "AND invl.product_group_id not in (" + str(excluded_product_groups).strip('[]') + ")"
query4 += '''
            order by ihead.invoice_no asc'''

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
    if DEBUG > 1:
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

    if DEBUG > 1:
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
    if row.SerialNumberID is None or row.SerialNumberID == "":
        ID1_Line += "NULL-Serial".ljust(20)
    else:
        ID1_Line += str(row.SerialNumberID).ljust(20)
        # Is this a RMA?
    if row.is_rma is not None:
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

    if DEBUG > 1:
        print ID1_Line
        print AD02_Line
        print AD08_Line

    return row.Price

# This function finds charges applied to the invoice as 'other charges', which show up in the total of the invoice
#   however are actually additions/subtractions to the subtotal and NOT line items.  These totals need to be
#   subtracted from the stored invoice_amount in order to have the line item totals and the invoice total to match.
#
def get_charges_to_exclude():

    # Return any logging messages for the email to send
    ret = ""

    cnxn5 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db_commerce_center_ip + ';DATABASE=' + db_commerce_center_name +
                           ';UID=' + db_commerce_center_UID + ';PWD=' + db_commerce_center_pwd)
    cnxn6 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db_commerce_center_ip + ';DATABASE=' + db_commerce_center_name +
                           ';UID=' + db_commerce_center_UID + ';PWD=' + db_commerce_center_pwd)
    cnxn7 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + db_commerce_center_ip + ';DATABASE=' + db_commerce_center_name +
                           ';UID=' + db_commerce_center_UID + ';PWD=' + db_commerce_center_pwd)

    # Query to exclude any items that are of type 'other charge' on the invoice
    query = '''
           select DISTINCT iline.invoice_no,
                iline.item_id,
                iline.unit_price,
                iline.qty_shipped,
                ihead.total_amount
        from
                invoice_line as iline
                INNER JOIN invoice_hdr as ihead
                    on ihead.invoice_no = iline.invoice_no'''
    if len(excluded_product_groups) > 0:
        query += '''
            inner join inv_mast invm on invm.inv_mast_uid = iline.inv_mast_uid
            inner join inv_loc invl on invl.inv_mast_uid = invm.inv_mast_uid
            '''
        query += '''
            WHERE CONVERT(varchar,ihead.invoice_date,112) = ''' + "'" + query_date + "'" + '''
            AND ihead.customer_id = ''' + "'" + ge_account_number + "'"

    if len(excluded_product_groups) > 0:
        query += ''' AND (iline.other_charge_item = 'Y' OR
                     invl.product_group_id in (''' + str(excluded_product_groups).strip('[]') + ") )"
    else:
        query += '''AND iline.other_charge_item = 'Y'
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

    # Query to exclude tax from an invoice
    query3 = '''
    select DISTINCT ihead.invoice_no,
                    ihead.tax_amount
        from
            invoice_hdr as ihead
        WHERE CONVERT(varchar,ihead.invoice_date,112) = '20150925'
            AND ihead.customer_id = '101411'
            AND ihead.tax_amount <> 0
    '''

    if DEBUG:
        print "Queries to pull other charges"
        print query
        print query2
        print query3

    # cursor has other_charge items
    cursor = cnxn5.cursor()
    cursor2 = cnxn6.cursor()
    cursor3 = cnxn7.cursor()

    # cursor2 handles latitude PPDADD freight
    #  or any freight that is a charge on the invoice (check freight_code table)
    cursor.execute(query)
    cursor2.execute(query2)
    cursor3.execute(query3)
    other_charge_sum = {}

    # Since we are using a dictionary, we must either add a new key or lookup the key and add to the invoice sum

    if cursor.rowcount < 0:
        ret += " ==> Adjusting for other charges detected on invoices\n"
        for row in cursor:
            ret += "\tInvoice " + str(row.invoice_no) + " was adjusted.\n"
            if row.invoice_no in other_charge_sum:
                other_charge_sum[row.invoice_no] += (row.unit_price * row.qty_shipped)
            else:
                other_charge_sum[row.invoice_no] = (row.unit_price * row.qty_shipped)
    else:
        ret += " --> No other charges items affect subtotal.\n"

    # Do the same for PPDADD freight
    if cursor2.rowcount < 0:
        ret += " ==> Adjusting for PPDADD freight (or any other freight) found on invoices.\n"
        for row in cursor2:
            ret += "\tInvoice " + str(row.invoice_no) + " was adjusted.\n"
            if row.invoice_no in other_charge_sum:
                other_charge_sum[row.invoice_no] += row.freight_out
            else:
                other_charge_sum[row.invoice_no] = row.freight_out
    else:
        ret += " --> No Latitude shipping charges affect subtotal.\n"

    if cursor3.rowcount < 0:
        print " ==> Adjusting for tax charges that were detected on invoices.\n"
        for row in cursor3:
            ret += "\tInvoice " + str(row.invoice_no) + " was adjusted.\n"
            if row.invoice_no in other_charge_sum:
                other_charge_sum[row.invoice_no] += row.tax_amount
            else:
                other_charge_sum[row.invoice_no] = row.tax_amount
    else:
        ret += " --> No Tax charges affect subtotal.\n"

    if DEBUG:
        print "Other charges returned:"
        for key in other_charge_sum:
            print "\tInvoice:\t" + str(key) + " Total charges:\t" + str(other_charge_sum[key])

    # Return the dictionary to the main program as well as the log, returned as a tuple to unpack
    return other_charge_sum, ret


def log_exclusions(row):

    ret = ""
    ID1_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "2".ljust(2)
    # Test for NULL serial numbers
    if row.SerialNumberID is None or row.SerialNumberID == "":
        ID1_Line += "NULL-Serial".ljust(20)
    else:
        ID1_Line += str(row.SerialNumberID).ljust(20)
        # Is this a RMA?
    if row.is_rma is not None:
        ID1_Line += str(row.Price)[:(str(row.Price)).index('.') + 3].translate(None, '.').rjust(11, '0') + '-' + \
            str(row.ItemID).split(' ')[0].ljust(4) + str(row.ItemID).split(' ')[1].ljust(60)
    else:
        ID1_Line += str(row.Price)[:(str(row.Price)).index('.') + 3].translate(None, '.').rjust(11, '0') + ' ' + \
            str(row.ItemID).split(' ')[0].ljust(4) + str(row.ItemID).split(' ')[1].ljust(59)
    ret += ID1_Line + '\n'

    # Additional Detail AD02
    AD02_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "DAD02" + str(row.item_desc).ljust(92)
    ret += AD02_Line + '\n'

    # Additional Detail AD08
    AD08_Line = str(row.Dealer).ljust(13) + str(row.invoice_no).ljust(10) + "DAD08" + str(row.ItemID).ljust(92)
    ret += AD08_Line + '\n'

    return ret


def check_invoices(qry, other_charges, invoice_dict,  exclusion_list, applied_oc):

    # We have 4 invocations of this function, if the other charges have already been applied, we should
    #   not apply them more than once.
    # Create a dictionary with the sum of the invoice items
    # --> 01-14-2016 - Change row.serialNumberID check from len() to 'is None'
    for row in qry:
        if row.invoice_no in invoice_dict:
            # If we have already seen this invoice once before....
            if row.SerialNumberID is not None:
                if row.SerialNumberID == "Null-Serial" and row.is_rma:
                    # Non-Serialized RMA case
                    #   No negative here because quantity is negative
                    invoice_dict[row.invoice_no] = invoice_dict[row.invoice_no] + row.Price * row.Qty
                elif row.is_rma:
                    # Serialized RMA case
                    #   Each will be on a separate line, no need to use quantity
                    invoice_dict[row.invoice_no] = invoice_dict[row.invoice_no] - row.Price
                else:
                    invoice_dict[row.invoice_no] = invoice_dict[row.invoice_no] + row.Price
            else:
                # TODO:  Determine if dead code, maybe put debug statement in and monitor?
                if row.is_rma:
                    invoice_dict[row.invoice_no] = invoice_dict[row.invoice_no] + row.Price * row.Qty  # * -1
                else:
                    invoice_dict[row.invoice_no] = invoice_dict[row.invoice_no] + row.Price * row.Qty
        # If we have not seen this invoice before, we need to do the initial add of it.
        else:
            if row.SerialNumberID is not None:
                if row.is_rma and row.SerialNumberID == "Null-Serial":
                    invoice_dict[row.invoice_no] = row.Price * row.Qty
                elif row.is_rma:
                    invoice_dict[row.invoice_no] = row.Price * -1
                else:
                    invoice_dict[row.invoice_no] = row.Price
            else:
                # Probably now dead code due to change in above 'if' condition
                if row.is_rma:
                    invoice_dict[row.invoice_no] = row.Price * -1 * row.Qty
                else:
                    invoice_dict[row.invoice_no] = row.Price * row.Qty

    # Handle other charges detected on the invoice
    if other_charges:
        for key in other_charges:
            # Do not apply any other_charge to an invoice more than once, if we used it
            if key in invoice_dict:
                if key in applied_oc:
                    continue
                else:
                    applied_oc.append(key)
                    invoice_dict[key] = invoice_dict[key] + other_charges[key]


def exclude_mismatches(qry, other_charges, invoice_dict, exclusion_list):

    ret = ""
    for row in qry:
        if row.invoice_no in exclusion_list:
            ret += log_exclusions(row)
            continue
        else:
            if row.invoice_no in invoice_dict:
                if row.Inv_Total != invoice_dict[row.invoice_no]:
                    ret += "--> Adding invoice " + str(row.invoice_no) + " to the exclusion list\n"
                    try:
                        ret += "\tInvoice total: " + str(row.Inv_Total) + " \tRow total: " + \
                            str(invoice_dict[row.invoice_no]) + \
                            "\tOther charges total: " + str(other_charges[row.invoice_no]) + "\n"
                    except KeyError:
                        ret += "\tInvoice total: " + str(row.Inv_Total) + " \tRow total: " + \
                            str(invoice_dict[row.invoice_no]) + "\tNo other charges\n"
                    ret += "================================================\n"
                    ret += "Lines from the excluded invoice below\n"
                    ret += "================================================\n"
                    exclusion_list.append(row.invoice_no)
                    ret += log_exclusions(row)
    if DEBUG:
        print ret
    return ret


# Send an email to report on batch status
def send_email_report(exclusion_log, adjustment_log, exclusion_list):
    import smtplib
    from email.mime.text import MIMEText
    if len(exclusion_list) > 0:
        msg = MIMEText(exclusion_log + '\n\n' + adjustment_log)
        msg['Subject'] = "GE Batch Report: Invoice excluded from batch dated " + query_date
    else:
        msg = MIMEText(exclusion_log + "Batch ran with with no exclusions detected\n\n" + adjustment_log)
        msg['Subject'] = "GE Batch Report: No exclusions detected for batch dated " + query_date

    msg['From'] = email_from_address
    msg['To'] = ', '.join(map(str, email_to_addresses))
    mail_server = smtplib.SMTP(email_server_ip)
    mail_server.sendmail(email_from_address, email_to_addresses, msg.as_string())
    mail_server.quit()


def print_non_serialized(invoice_no, batch_total):
    if len(rma_non_serial) > 0:
        for row in rma_non_serial:
            # Skip any out of balance invoice so we don't send an invalid file
            if row.invoice_no in exclusion_list or row.invoice_no != invoice_no:
                continue
            num_to_print = abs(row.Qty)
            while num_to_print > 0:
                batch_total = batch_total - item_detail(row)
                num_to_print -= 1
                if DEBUG >= 10:
                    print "item price:", str(row.Price), "\tQuantity:", str(row.Qty), "\t\trunning batch total: ", \
                        str(batch_total), row.ItemID, row.item_desc
    return batch_total

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
other_charges, ADJUST_LOG = get_charges_to_exclude()

if DEBUG > 1:
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
exclusion_list = []         # list of any invoices to exclude due to detected inconsistencies
printed_list = []           # list of non-serialzed RMA items printed in the serialzed loop
invoice_dict = {}
applied_oc = []

# FOR NOW, SKIP EXCLUSIONS
check_invoices(rows, other_charges, invoice_dict, exclusion_list, applied_oc)
check_invoices(non_latitude_rows, other_charges, invoice_dict, exclusion_list, applied_oc)
check_invoices(rma_rows, other_charges, invoice_dict, exclusion_list, applied_oc)
check_invoices(rma_non_serial, other_charges, invoice_dict, exclusion_list, applied_oc)

EXCLUSION_LOG += exclude_mismatches(rows, other_charges, invoice_dict, exclusion_list)
EXCLUSION_LOG += exclude_mismatches(non_latitude_rows, other_charges, invoice_dict, exclusion_list)
EXCLUSION_LOG += exclude_mismatches(rma_rows, other_charges, invoice_dict, exclusion_list)
EXCLUSION_LOG += exclude_mismatches(rma_non_serial, other_charges, invoice_dict, exclusion_list)

if DEBUG:
    print "========================================| Loop 1"
    print exclusion_list

# Latitude-shipped sales
for row in rows:
    # Skip any out of balance inovice so we don't send an invalid file
    if row.invoice_no in exclusion_list:
        continue
    # If the item is serialized, we only print one, otherwise we have to respect the quantity
    if row.SerialNumberID is not None:
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
        if DEBUG >= 10:
            print "item price:", str(row.Price), "\tQuantity:", str(row.Qty), "\t\trunning batch total: ", str(batch_total), \
                row.ItemID, row.item_desc

if DEBUG:
    print "========================================| Loop 2"

# Non-Latitude sales
#  - Since the query does not pull one line per item, we use the quantity field when printing
if len(non_latitude_rows) > 0:
    for row in non_latitude_rows:
        # Skip any out of balance inovice so we don't send an invalid file
        if row.invoice_no in exclusion_list:
            continue
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
        if DEBUG >= 10:
            print "item price:", str(row.Price), "\tQuantity:", str(row.Qty), "\t\trunning batch total: ", str(batch_total), \
                row.ItemID, row.item_desc

if DEBUG:
    print "========================================| Loop 3"

# RMA Loop.  Since these are all returns we can switch to pure subtraction
#  03-25-2016 - Added printing of all non-serialzed items to prevent duplicate invoice headers
#             ---> Since RMAs can have both serialized and non-serialized items, it was possible to have
#             --->   multiple invoice header records emitted in the export file.  Printing the
#             --->   non-serialized items for each RMA with serialized items prevents this.
#             --->   We still need the last loop for printing non-serialized only RMA's
if len(rma_rows) > 0:

    for row in rma_rows:
        # Skip any out of balance invoice so we don't send an invalid file
        if row.invoice_no in exclusion_list:
            continue
        # Check to see if we need a new invoice header record or if it is the same invoice
        if current != row.invoice_no:
            header_count += 1
            invoice_total += row.Inv_Total
            current = row.invoice_no
            invoice_header(row, other_charges)
            address_info(row)
            batch_total = batch_total - item_detail(row)
            batch_total = print_non_serialized(row.invoice_no, batch_total)
            printed_list.append(row.invoice_no)
        else:
            batch_total = batch_total - item_detail(row)
        if DEBUG >= 10:
            print "item price:", str(row.Price), "\tQuantity:", str(row.Qty), "\t\trunning batch total: ", \
                str(batch_total), row.ItemID, row.item_desc

if DEBUG:
    print "========================================| Loop 4"

# RMA non-serialized
#  - Since the query does not pull one line per item, we use the quantity field when printing
if len(rma_non_serial) > 0:
    for row in rma_non_serial:
        # Skip any out of balance invoice so we don't send an invalid file
        if row.invoice_no in exclusion_list or row.invoice_no in printed_list:
            continue
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
            if DEBUG >= 10:
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
    send_email_report("No activity detected, no file sent\n", ADJUST_LOG, exclusion_list)
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
    if DEBUG >= 1:
        print file_footer
    if DEBUG:
        print "Invoice Totals: ", invoice_total, "\t\tItem Totals: ", batch_total

# Close the file so we can send it....
output_file.close()
print "Export complete...."

# Log any exclusions here
if len(exclusion_list) > 0:
    exclusions_file_name = "Exclusions_" + query_date
    print "INVOICES EXCLUDED:  See the log file"
    outstring = ', '.join(exclusion_list)
    exclusions_file = open(exclusions_file_name, "w")
    exclusions_file.write(EXCLUSION_LOG)
    exclusions_file.close()
    shutil.copy2(exclusions_file_name, archive_path)
    os.remove(exclusions_file_name)

# Put the file in the temporary FTP location
shutil.copy2(output_file_name, temp_path)

if DEBUG:     # If we are debugging, skip the transfer
    DEBUG_LOG = "THIS EMAIL COMES FROM A DEBUGGING RUN OF THE PROGRAM\n"
    DEBUG_LOG += "NO FILE WAS SENT FOR PROCESSING\n"
    DEBUG_LOG += "=====================================================\n"
    DEBUG_LOG += "Start of email\n"
    DEBUG_LOG += "=====================================================\n"
    DEBUG_LOG += EXCLUSION_LOG
    send_email_report(DEBUG_LOG, ADJUST_LOG, exclusion_list)
    print "Transfer skipped due to debugging...archiving activity file...."
    shutil.copy2(output_file_name, archive_path)
    os.remove(temp_path + '/' + output_file_name)
    os.remove(output_file_name)
    print "Archive complete....exiting."
    exit(0)

# Replace forward slashes with back slashes for command line commands
print "Starting file transfer...."
transfer_cmd = win_scp_path + ' /script=WinSCP-commands.cmd ' + \
    ' /xmllog=' + ftp_log_path.replace('/', '\\') + '\!Y-!M-!D-!T-log.xml /log=' + ftp_log_path.replace('/', '\\') + \
    '\!Y-!M-!D-!T-log.txt' + ' /parameter ' + output_file_name + ' ' + temp_path + ' ' + ftp_username + ' ' + \
    ftp_password + ' ' + ftp_url

#TODO: Create custom email in case FTP fails?
if subprocess.call(transfer_cmd):
    print "An error occurred during FTP transfer.  This was logged in " + ftp_log_path
    exit(-1)
else:
    send_email_report(EXCLUSION_LOG, ADJUST_LOG, exclusion_list)
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
