import psycopg2, csv, time

dbConnection = psycopg2.connect("dbname=mike user=mike")



def insertRow(row):
    global dbConnection
    dbCursor = dbConnection.cursor()
    result = False

    try:
        dbCursor = dbConnection.cursor()
        try:
            dbCursor.execute("INSERT INTO \"public\".\"iucr_reference\"(\"IUCR\",\"Primary Type\",\"Description\") VALUES(%s, %s, %s)", tuple(row))
        except psycopg2.IntegrityError:
            dbConnection.rollback()
        else:
            dbConnection.commit()
            result = True

        dbCursor.close()
    except Exception, e:
        print 'ERROR: ',e[0]
        exit(0)

    return result

with open('iucr.csv', 'rb') as iucrCsv:
    startTime = time.time()
    print "Parse begun"
    csvReader = csv.reader(iucrCsv, delimiter=',', quotechar='"')
    maxRows = 12
    rowsRead = 0
    rowsInserted = 0
    rowsDuplicate = 0
    seenHeader = False
    for row in csvReader:
        if seenHeader is not True:
            seenHeader = True
            continue
        else:
            rowsRead = rowsRead + 1
            # maxRows = maxRows - 1

            if insertRow(row) is True:
                rowsInserted = rowsInserted + 1
            else:
                rowsDuplicate = rowsDuplicate + 1

            if rowsRead % 100000 == 0:
                print 'Progress: Read ', rowsRead, ' rows'

            if maxRows < 0:
                break

    dbConnection.close()
    endTime = time.time()
    print "Read ", rowsRead, " rows"
    print "Inserted ", rowsInserted, " rows"
    print rowsDuplicate, " duplicate rows identified"
    print "Operation took ", round(endTime - startTime, 3), " seconds"
