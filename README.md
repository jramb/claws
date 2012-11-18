CLAWS - Clojure Library for Amazon Web Services
===============================================

* Note: 2012-11-18: This is very, very unfinished and it might never be. Don't use it.

Introduction
------------

This library is my shot at making a wrapper library for
some of the Amazon Web Services. To start with: it is NOT
complete. It is NOT ready for production. I have barely started
and expect major rewrites.

My current goal is to implement my personal needs to start with.
I scratch my own itches. This is work in progress for now, hopefully I
can reach a level where CLAWS could be useable for others as well.

You will need
-------------
1. Leiningen
2. An AWS account


Examples
--------
(Working right now...)
Clone the repo and do a `lein deps`.

Create a file `src/AwsCredentials.properties` with your credentials:

    # Fill in your AWS Access Key ID and Secret Access Key
    # http://aws.amazon.com/security-credentials
    accessKey = XXXXXXXXXXXXXXXXXXXX
    secretKey = yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy

Keep this file for yourself, otherwise you might loose money...
Just saying it.

Now you you can play with for example DynamoDB, it is assumed that
you already created a table called `my-table`:

    (ns demo.space
        (:require [claws.dynamodb :as d]))

    ;; create a client with default credentials (according your property file)
    (def dyndb (d/dynamodb-client))
    
    ;; write a record to your table.
    ;; records are hash maps can have either String och keyword keys.
    ;; The values are either number, String, sequence of numbers or sequence of Strings
    ;; (only unique values are accepted).
    ;; Sequences may not mix types, i e you can not have boths numbers and Strings in one of them.
    (d/put-item dyndb "my-table"
      {:hash-key "test-2"
       "a" 99
       "b" "Hej!!"
       :c [4 5 6 7]
       :d ["Hipp" "Hopp" "Hej" "Jörg"]})

    ;; and read it
    (d/get-item dyndb "my-table" "Test2")
    ;; -> {:hash-key "test-2"
       :a 99
       :b "Hej!!"
       :c #{4 5 6 7}
       :d #{"Hipp" "Hopp" "Hej" "Jörg"}}


The code is extremely compact compared to the examples
given on the Amazon documentation...

More examples can be found in the [tests](test/claws/test/dynamodb.clj).

Planned
-------
1. DynamoDB support
  * More sophisticated select and updates
  * Create and modify tables
1. S3 support
1. Maybe SimpleDB support
1. Maybe SQS, SNS, SWF
1. more, if there is interest.

