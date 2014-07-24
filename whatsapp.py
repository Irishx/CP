import os, re, codecs, sys
from pynlpl.formats import folia
from datetime import datetime

#dit is python3 want dat lost de unicode problemen ook meteen op

def process(l):
    "split the whatsapp line into its parts"
    a = line[:-1].split("\t") #or a = [" ", " ", " "]
    date = a[0] 
    author = a[1] 
    message = a[2]
   
    #print(anonymous)
    return (date, author, message)



# read in meta data
metafile = "/vol/bigdata/corpora/CMC/Whatsapp2013/Files_gestandaardiseerd/metadata.txt"
#dictionary= hash
users = {}
# make a list of all nicknames and their anonymous versions
array = []
for line in open(metafile):
    array = line[:-1].split("\t")
    users[array[3]] = array[0]
print(users)

mydir = "/vol/bigdata/corpora/CMC/Whatsapp2013/Files_gestandaardiseerd/"
# read in a directory with whatsapp files ending in 
for filename in os.listdir(mydir):
    print(filename)
    if filename.endswith("gestandaardiseerd.txt"):
        print(filename)

        #open the whatsapp file
        f = open(os.path.join(mydir,filename))
        lines = f.readlines()
        docstr = filename

        # create a folia document with a numbered id
        doc = folia.Document(id=docstr)
        doc.declare(folia.Event, "hdl:1839/00-SCHM-0000-0000-000A-B")     
        # first create an folia text opbject, then paste string into it
        text = doc.append(folia.Text)


# iterating over the lines, while keeping a counter
#for i in range(len(lines)):
 #   print i, lines[i]

        mybegindate = myactor = mymessage = ' '
        messagecounter = 0    
        # lees regel voor regel uit bestand
        for line in lines:
            line= line.replace('\ufeff', '')
  
            print(line)
            # als regel begint met [datum]
            # schrijf message-event weg
            # maak nieuwe message-event
            resultdate = re.search('.+\d+:\d\d', line)
            if (resultdate):
            # print the previous message befor processing the current one
            #print("previous message")
            #print(mybegindate, myactor, mymessage)
                eventid = "text.%(docstr)s.event.%(messagecounter)s" % vars()
       
                if(messagecounter>0):
                    if users[myactor]:
                        anonymous = users[myactor]
                        print(anonymous)
                        chatevent = folia.Event(doc, id=eventid, actor=anonymous, cls="message", begindatetime=mybegindate, text=mymessage)
                        text.append(chatevent)
                    else:
                        print("what is wrong",myactor)
 
                messagecounter += 1
                #print("message %(messagecounter)s" % vars())
                (mybegindate, myactor, mymessage) = process(line)
                # if no resultdate, append line to previous message
            else:
                mymessage += "\n"
                mymessage += line
  

        # dont forget the last line!
        eventid = "text.%(docstr)s.event.%(messagecounter)s" % vars()
        anonymous = myactor
        chatevent = folia.Event(doc, id=eventid, actor=anonymous, begindatetime=mybegindate, text=mymessage)
        text.append(chatevent)

        outfile = docstr + ".folia.xml"
        doc.save(outfile)




