WARC/1.1
WARC-Type: revisit
WARC-Record-ID: 
Content-Length: 0 [Unless http response is available??]
WARC-Date: YYYY-MM-DDThh:mm:ssZ [Date of the revisit]                                              
Content-Type: application/html [Only if http response headers are available]
WARC-Block-Digest: sha1: ... [Only if http response headers are available]
WARC-Payload-Digest: sha1: ... [Digest of the record which is being revisited]
WARC-IP-Address:
(WARC-Refers-To: [We probably don't use this])
WARC-Target-URI: http://.
WARC-Profile: http://netpreserve.org/warc/1.0/revisit/identical-payload-digest
WARC-Refers-To-Target-URI: []
WARC-Refers-To-Date: [Date when archived content was retrieved]
WARC-Truncated: length

HTTP/1.x .........  [http response if available]

In practice, records generated from deduplications will not have an http response, so they will look like

WARC/1.1
WARC-Type: revisit
WARC-Record-ID: 
Content-Length: 0
WARC-Date: YYYY-MM-DDThh:mm:ssZ [Date of the revisit]                                              
WARC-Payload-Digest: sha1: ... [Digest of the record which is being revisited]
WARC-IP-Address:
WARC-Target-URI: http://.
WARC-Profile: http://netpreserve.org/warc/1.0/revisit/identical-payload-digest
WARC-Refers-To-Target-URI: [Usually the same as WARC-Target-URI]
WARC-Refers-To-Date: [Date when archived content was retrieved]
WARC-Truncated: length

Our deduplication records look like:
2010-01-24T11:15:39.131Z   200      10586 http://images.bt.dk/node-images/107/300x250-l/107938-stjernesyge-slankekure---se-hvad-de-kendte-gr-for-at-tabe-sig--.jpg XLLLE http://www.bt.dk/underholdning/stjernesyge-slankekure-se-hvad-de-kendte-goer-tabe-sig image/jpeg #087 20100124111539085+45 sha1:25DCVSHR7OAMYRDZMK2AEQRMJ7PC5BDO - duplicate:"9-6-20100123114900-00003-sb-test-har-001.statsbiblioteket.dk.arc,36167170",content-size:10970
So they are missing the WARC-Refers-To-Date and also the filename and offset need to be updated for the compressed archive. Therefore, in order to support revisit records,
 the i-file needs to contain the WARC-Date of the record as well as the old and new offsets.
 
