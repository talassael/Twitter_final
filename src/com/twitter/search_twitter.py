import pymongo , Queue  , logging , time , ConfigParser , thread , sys , inspect , urllib, urllib2, json


def search():
    logging.basicConfig(filename='twitter_search.log',level=logging.DEBUG)
    logging.info('*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-')
    logging.info(str(time.ctime()) + ' : starting "search" modul')
    logging.info(str(time.ctime()) + ' : getting data from properties file')
    cf = ConfigParser.ConfigParser() #  object for reading properties file
    cf.read("twitterconfig.properties") #   read properties file
    percentage = cf.get('sec1','percentage').split(',') #   get the percentage array
    initial_search_terms = cf.get('sec1','searchtable').split(',') #    get the initial search terms array
    max_queue = int(cf.get('sec1','max_queue')) #    get the max queue data
    min_terms = int(cf.get('sec1','min_terms')) #    get the min term data
    logging.info(str(time.ctime()) + ' : min_terms = ' + str(min_terms) + ' , max_queue = ' + str(max_queue))
    logging.info('initial_search_terms = ' + str(initial_search_terms))
    logging.info('percentage = ' + str(percentage))
    percents = {}
    for i in range(0,len(percentage),2):
        percents.setdefault(i/2,time.time())
    logging.info(str(time.ctime()) + ' : percents initial dictionary = ' + str(percents)) #log
    logging.info(str(time.ctime()) + ' : starting search process , connecting to mongodb') #log
    connection = pymongo.Connection("localhost", 27017) # connect to mongodb
    logging.info(str(time.ctime()) + ' : connecting to the "twitter" DB') #log
    db = connection.twitter # connect to "twitter" db
    logging.info(str(time.ctime()) + ' : creating a queue for search terms') #log
    queue = Queue.Queue(max_queue)  #create a queue for search terms
    logging.info(str(time.ctime()) + ' : starts searching endless loop') #log
    count_searches = 0
    while(1):
        
        if (queue.empty()):
            to_limit = db.search_terms.count() - 5000
            if(to_limit > 0):
                terms = db.search_terms.find().sort('over_all' , 1).limit(to_limit)
                for term in terms:
                    db.search_terms.remove({'search_term' : term['search_term']})
            count = db.search_terms.count() # check how many search terms with more then 1 there are in db
            logging.info(str(time.ctime()) + ' : queue is empty, calling function "fill_queue"') #log
            queue , percents = fill_queue(initial_search_terms , count , max_queue , min_terms, percents , percentage) # if queue is empty, fill it
        count_searches = count_searches + 1
        newterm = queue.get() # if queue is not empty, get next search word
        logging.info(str(time.ctime()) + ' : next search term to process by api: ' + newterm[0] + '    -- This is search number: ' + str(count_searches)) #log
        max_id = searchtweets(newterm[0], newterm[1]) # call twitter search api for that search word
        db.search_terms.update({'search_term':newterm[0]} , {"$set" :{'max_id':max_id}})           
    logging.info(str(time.ctime()) + ' : end "search"')
    logging.info('**********************************************************************************************************************')


def fill_queue(initial_search_terms , count , max_queue , min_terms , percents , percentage):
    logging.basicConfig(filename='twitter_search.log',level=logging.DEBUG)
    logging.info(str(time.ctime()) + ' : starting "fill_queue" , creating a queue') #log
    logging.info(str(time.ctime()) + ' : there are ' + str(count) + ' search terms in collection') #log
    queue = Queue.Queue(max_queue)
    count_inserted = 0
    connection = pymongo.Connection("localhost", 27017) # connect to mongodb
    logging.info(str(time.ctime()) + ' : connecting to the "twitter" DB') #log
    db = connection.twitter # connect to "twitter" db
    num_of_perc = []
    if (count < int(min_terms)): #   if there is still not enough terms in table
        logging.info(str(time.ctime()) + ' : not enough terms in DB, filling queue from configuration file') #log
        for term in initial_search_terms:
            queue.put((term,0))     # put the initial search words into the queue
    else:
        logging.info(str(time.ctime()) + ' checking where which terms to add to queue') #log
        sorted_percentage = sorted(percents.iteritems(), key=lambda (k,v):(v,k)) #  sort the percent dictionary by the earliest time to sample
        current_percentage = sorted_percentage[0][0]    #   get the earliest percentage to sample
        logging.info(str(time.ctime()) + ' : current percentage is ' + str(sorted_percentage[0][0])) #log
        print 'current percentage is ' + str(sorted_percentage[0][0])
        logging.info(str(time.ctime()) + ' : filling the queue with the terms from the right percentage') #log
        for i in range(0, (len(percentage)/2), 1):
            num_of_perc.append(int(percentage[i*2])*count/100) 
        if (current_percentage == 0):
            to_skip = 1
        else:
            to_skip = 0
            for i in range(current_percentage):
                to_skip = to_skip + num_of_perc[i]
        limit = num_of_perc[current_percentage]
        terms = db.search_terms.find().sort("over_all" , -1).skip(to_skip).limit(limit)
        for term in terms:
            if (term['search_term'] == ''):
                db.search_terms.remove(term)
            else:
                for i in range(limit):
                    queue.put((term['search_term'] , term['max_id'])) #    insert term to queue
                    count_inserted = count_inserted + 1
        logging.info(str(time.ctime()) + ' : ' + str(count_inserted) + ' search terms inserted to queue, from percentage: ' + str(current_percentage) + ' which is ' + str(percentage[current_percentage*2]) + '%.')
        #if (time.time() > sorted_percentage[0][1] + 300): #   current time is earlier then sample time
        #    logging.info(str(time.ctime()) + ' : going to sleep for - ' + str(time.time()-sorted_percentage[0][1]) + ' seconds.')
        #    time.sleep(time.time()-sorted_percentage[0][1]) #   sleep for the delta of times
        percents[current_percentage] = time.time() + (3600*(long(percentage[current_percentage*2+1]))) #   update next sample time for percentage
        logging.info(str(time.ctime()) + ' time for next sample of percentage number: ' + str(current_percentage) + ' is: ' + str(time.ctime(percents[current_percentage]))) #log
    logging.info(str(time.ctime()) + ' : end "fill_queue"')
    logging.info('**********************************************************************************************************************')
    return queue , percents


def searchtweets(query,since_id='',):
    logging.basicConfig(filename='twitter_search.log',level=logging.DEBUG)
    logging.info(str(time.ctime()) + ' : starting twitter apis process , searching for term: ' + str(query)) #log
    count_total = 0     # count total results for function
    #print 'searching for term: ' + str(query)
    connection = pymongo.Connection("localhost", 27017) # connect to mongodb
    db = connection.twitter     # connect to "twitter" DB
    logging.info(str(time.ctime()) + ' : starting loop of 15 calls to api') #log
    since_id_prior = since_id
    for i in range(1,16,1):     # run search 15 times - unless no more results
        logging.info(str(time.ctime()) + ': iteration number ' + str(i+1) +  ' , since_id is: ' + str(since_id)) #log
        params = urllib.urlencode(dict(q=query, rpp=100, since_id=since_id_prior, page=i,include_entities=True))        # parameters for twitter search api
        #print 'starting search number: ' + str(count) + ' term: ' + query
        logging.info(str(time.ctime()) + ' : calling api') #log
        try:
            u = urllib2.urlopen('http://search.twitter.com/search.json?' + params , data=None , timeout = 10)       # call api
        except Exception , e:
            logging.error(e)
        try:
            j = json.load(u)        # load the api answer to json
        except Exception , e:
            logging.error(e)
            data = { 'empty_doc':'true' } 
            data_string = json.dumps(data)
            j = json.loads(data_string)
            #print str(e)
            #j = json.load('null')
        logging.info(str(time.ctime()) + ' : adding search time to json') #log
        currenttime = time.time()
        j.update({'query_time':currenttime})       # adding current time as search time to json
        j.update({'query_time_string':time.ctime(currenttime)})       # adding current time as search time to json
        logging.info(str(time.ctime()) + ' : saving to DB') #log
        db.raw_data.save(j)        # save json to DB
        try:
            since_id = j['max_id']      # max id is since id of next iteration
        except KeyError:
            logging.info(str(time.ctime()) + ' : no results found by api, since_id is 0') #log
            since_id = 0
        
        #print 'search number: ' + str(count) + ', max_id = ' + str(j['max_id']) + ',  ' + str(len(j['results'])) + ' results'
        if (j.has_key('results')):
            count_total = count_total + len(j['results'])       # calculate number of results so far
            if len(j['results']) < 50:
                logging.info(str(time.ctime()) + ' : page has ' + str(len(j['results'])) + ' results, which means thats all , breaking the loop') #log
                break
        else:
            break
           # pass
        
        
        j.clear()       # clear json
        u.close()
        logging.info('**********************************************************************************************************************')
    print str(count_total) + ' results found for: ' + str(query)
    #pprint.pprint(j)
    #a = searchtweets.searchtweets('israel')
    """
        import pymongo
        connection = pymongo.Connection("localhost", 27017)
        db = connection.test
        for i in range(13):
            j = searchtweets.searchtweets('israel',f)
            f = j.get('max_id')
            j.update({'query_time':time.ctime()})
            db.raw_data.save(j)


	a = db.raw_data.find( { 'completed_in' : 0.2 } )
	for s in a:
            print s

        for result_object in a:
            print result_object['results'][0]  # Mongo ObjectId of the result_object
    """
    logging.info(str(time.ctime()) + ' : end "searchtweets", ' + str(count_total) + ' results for ' + str(query) + '.')
    logging.info('**********************************************************************************************************************')
    return since_id

def initiate_current_slot():
    connection = pymongo.Connection("localhost", 27017)
    db = connection.twitter
    time_millis = time.time()
    if (db.current_slot.count() == 0):
        db.current_slot.insert({'current_slot' : 0 , 'slot_start_time' : str(time_millis*1000) , 'slot_start_time_string':time.ctime(time_millis)})
    
            
        
initiate_current_slot()        
search()      


    
    #    ('israel','lebanon','jordan','syria','kenya','usa','germany','ethiopia','nigeria','france','greece','obama','bibi','barak','bolt','armstrong','jobs','gates','water','air','table','wall','computer','software','data','program','tv','channel','tunnel','work','life','lifestyle','home','food','shoes','clothes','football','soccer','nba','basketball')
