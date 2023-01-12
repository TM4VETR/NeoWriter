import sys
from optparse import OptionParser
import logging
from cassis import *
import csv
import hashlib
from py2neo import Graph

# Option Parser

parser = OptionParser()
parser.add_option("-c", "--connection", dest="connection", action="store", type="string", help="The connection", metavar="connection")
parser.add_option("-u", "--user", dest="user", action="store", type="string", help="The user", metavar="user")
parser.add_option("-p", "--password", dest="password", action="store", type="string", help="The password", metavar="password")
parser.add_option("-l", "--log-file", dest="logfile", default="pipeline.log", action="store", type="string", help="The logfile", metavar="logfile")
parser.add_option("-d", "--debug",
                  action="store_true", dest="debug", default=False,
                  help="Debug mode")                  
args = parser.parse_args()
(options, args) = parser.parse_args()

# Logging
ll = logging.INFO
if options.debug:
  ll = logging.DEBUG
logging.basicConfig(filename=options.logfile, encoding='utf-8', level=ll)

logging.info("===== NeoWriter =======")

# Graph connection
graph = Graph(options.connection, auth=(options.user,options.password))

c = ""
for line in sys.stdin:
    c = c + line
    
# Default Typesystem
typesystem=load_dkpro_core_typesystem()
# make cas out of stdin
cas = load_cas_from_xmi(c, typesystem=load_dkpro_core_typesystem())


idt = str(int(hashlib.sha1(cas.sofa_string.encode("utf-8")).hexdigest(), 16) % (10 ** 8))

q= 'CREATE (n:Document {id:"'+idt+'", type: "SteA", text: "'+cas.sofa_string+'"})'
logging.debug("==> "+q)
ret = graph.run(q).data()
logging.debug("==> "+str(ret))


for token in cas.select('de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity'):
  if "ESCO" in token.value:
    uri = token.value.split(":",1)[1]
  else:
    uri = token.value
  q = 'MATCH (a:Document), (b) WHERE a.id = "'+idt+'" AND b.uri= "'+uri+'" CREATE (a)-[r:hasTerm]->(b) RETURN type(r)'
  ret = graph.run(q).data()
  logging.debug("==> "+str(ret))
  #  writer.writerow ([cas.sofa_string[int(token.begin):int(token.end)], token.begin, token.end, token.value])

logging.info("===== NeoWriterEnde =======")
