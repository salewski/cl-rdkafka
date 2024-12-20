;;; Copyright (C) 2018-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
;;; Copyright 2022, 2024 Google LLC
;;;
;;; This file is part of cl-rdkafka.
;;;
;;; cl-rdkafka is free software: you can redistribute it and/or modify
;;; it under the terms of the GNU General Public License as published by
;;; the Free Software Foundation, either version 3 of the License, or
;;; (at your option) any later version.
;;;
;;; cl-rdkafka is distributed in the hope that it will be useful,
;;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;; GNU General Public License for more details.
;;;
;;; You should have received a copy of the GNU General Public License
;;; along with cl-rdkafka.  If not, see <http://www.gnu.org/licenses/>.

(in-package #:cl-user)

(defpackage #:test/high-level/consumer
  (:use #:cl #:1am #:test))

(in-package #:test/high-level/consumer)

(test consumer-subscribe
  (with-topics ((topic-1 "consumer-subscribe-topic-1")
                (topic-2 "consumer-subscribe-topic-2"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "consumer-subscribe-group"
                                 "enable.auto.commit" "true"
                                 "auto.offset.reset" "earliest")))
          (expected (list topic-1 topic-2)))
      (kf:subscribe consumer expected)
      (is (equal expected (sort (kf:subscription consumer) #'string<)))

      (kf:unsubscribe consumer)
      (is (zerop (length (kf:subscription consumer)))))))

(test poll
  (with-topics ((topic "consumer-poll-topic"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "consumer-poll-group"
                                 "enable.auto.commit" "true"
                                 "auto.offset.reset" "earliest")
                     :value-serde (lambda (x)
                                    (babel:octets-to-string x :encoding :utf-8))))
          (expected '("Hello" "World" "!")))
      (uiop:run-program
       (format nil "echo -n '~A' | kcat -P -D '|' -b '~A' -t '~A'"
               (reduce (lambda (agg s) (format nil "~A|~A" agg s)) expected)
               *bootstrap-servers*
               topic)
       :force-shell t
       :output t
       :error-output :output)
      (sleep 2)

      (kf:subscribe consumer (list topic))
      (sleep 5)
      (is (equal expected (loop
                             repeat (length expected)
                             for message = (kf:poll consumer 5000)
                             collect (kf:value message)))))))

(test seek
  (with-topics ((topic "consumer-seek-topic"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "consumer-seek-group"
                                 "enable.auto.commit" "false"
                                 "auto.offset.reset" "earliest")
                     :serde #'babel:octets-to-string))
          (expected '("one" "two" "three")))
      (uiop:run-program
       (format nil "echo -n '~A' | kcat -P -D '|' -b '~A' -t '~A'"
               (reduce (lambda (agg s) (format nil "~A|~A" agg s)) expected)
               *bootstrap-servers*
               topic)
       :force-shell t
       :output t
       :error-output :output)
      (sleep 2)
      (kf:subscribe consumer topic)
      (sleep 5)
      (is (equal expected
                 (loop
                   for message = (kf:poll consumer 5000)
                   while message
                   collect (kf:value message)
                   do (kf:commit consumer))))
      (kf:seek consumer topic 0 0 5000)
      (is (equal expected
                 (loop
                   for message = (kf:poll consumer 5000)
                   while message
                   collect (kf:value message)
                   do (kf:commit consumer))))
      (kf:seek-to-beginning consumer topic 0 5000)
      (is (equal expected
                 (loop
                   for message = (kf:poll consumer 5000)
                   while message
                   collect (kf:value message)
                   do (kf:commit consumer))))
      (kf:seek-to-beginning consumer topic 0 5000)
      (kf:seek-to-end consumer topic 0 5000)
      (is (null (loop
                  for message = (kf:poll consumer 5000)
                  while message
                  collect (kf:value message)
                  do (kf:commit consumer)))))))

(test committed
  (with-topics ((topic "consumer-committed-topic"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "consumer-commit-group"
                                 "enable.auto.commit" "false"
                                 "auto.offset.reset" "earliest")))
          (expected '(1 2 3)))
      (uiop:run-program
       (format nil "echo -n 'Live|Laugh|Hack' | kcat -P -D '|' -b '~A' -t '~A'"
               *bootstrap-servers*
               topic)
       :force-shell t
       :output t
       :error-output :output)
      (sleep 2)

      (kf:subscribe consumer (list topic))
      (sleep 5)
      (is (equal expected
                 (loop
                    with assignment = (kf:assignment consumer)
                    repeat (length expected)
                    do
                      (kf:poll consumer 5000)
                      (kf:commit consumer)
                    collect (cadar (kf:committed consumer assignment 5000))))))))

(test commit-sync
  (with-topics ((topic "consumer-test-commit-sync"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "consumer-commit-sync-group"
                                 "enable.auto.commit" "false"
                                 "auto.offset.reset" "earliest")))
          (expected `(((,topic . 0) . (0 . #(2 4 6)))
                      ((,topic . 0) . (1 . #(8 10 12)))
                      ((,topic . 0) . (2 . #())))))
      (is (equalp expected (kf:commit consumer :offsets expected))))))

(test commit-async
  (with-topics ((topic "consumer-test-commit-async"))
    (let* ((consumer (make-instance
                      'kf:consumer
                      :conf (list "bootstrap.servers" *bootstrap-servers*
                                  "group.id" "consumer-commit-async-group"
                                  "enable.auto.commit" "false"
                                  "auto.offset.reset" "earliest")))
           (expected `(((,topic . 0) . (0 . #(2 4 6)))
                       ((,topic . 0) . (1 . #(8 10 12)))
                       ((,topic . 0) . (2 . #()))))
           (future (kf:commit consumer :asyncp t :offsets expected)))
      (is (typep future 'kf:future))
      (is (equalp expected (kf:value future))))))

(test assign
  (with-topics ((topic "consumer-assign-topic"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "consumer-assign-group"
                                 "enable.auto.commit" "true"
                                 "auto.offset.reset" "earliest")))
          (partition 35))
      (kf:assign consumer (list (cons topic partition)))
      (destructuring-bind
            (actual-topic . actual-partition) (first (kf:assignment consumer))
        (is (string= topic actual-topic))
        (is (= partition actual-partition))))))

(test assign-offsets
  (with-topics ((topic "consumer-assign-offsets-topic"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "consumer-assign-offsets-group"
                                 "enable.auto.commit" "true"
                                 "auto.offset.reset" "earliest")))
          (partition-1 35)
          (partition-2 36)
          (offset 3))
      (kf:assign consumer (list (cons topic partition-1)
                                (cons (cons topic partition-2) offset)))
      (destructuring-bind
          (((actual-topic-1 . actual-partition-1) . actual-offset-1)
           ((actual-topic-2 . actual-partition-2) . actual-offset-2))
          (kf:assignment consumer :offsetsp t)
        (is (string= topic actual-topic-1))
        (is (string= topic actual-topic-2))
        (is (or (= partition-1 actual-partition-1)
                (= partition-2 actual-partition-1)))
        (if (= partition-1 actual-partition-1)
            (progn
              (is (= partition-2 actual-partition-2))
              (is (= cl-rdkafka/ll:rd-kafka-offset-stored actual-offset-1))
              (is (= offset actual-offset-2)))
            (progn
              (is (= partition-1 actual-partition-2))
              (is (= offset actual-offset-1))
              (is (= cl-rdkafka/ll:rd-kafka-offset-stored actual-offset-2))))))))

(test consumer-member-id
  (with-topics ((topic "consumer-member-id"))
    (let* ((group "consumer-member-id-group")
           (consumer (make-instance
                      'kf:consumer
                      :conf (list "bootstrap.servers" *bootstrap-servers*
                                  "group.id" group))))
      (kf:subscribe consumer (list topic))
      (sleep 5)

      (let ((member-id (kf:member-id consumer))
            (group-info (first (kf::group-info consumer group))))
        (is (find member-id
                  (cdr (assoc :members group-info))
                  :test #'string=
                  :key (lambda (alist)
                         (cdr (assoc :id alist)))))))))

(test consumer-pause
  (with-topics ((topic "consumer-pause-topic" t))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "consumer-pause-group"
                                 "auto.offset.reset" "earliest"
                                 "enable.auto.commit" "false")
                     :serde (lambda (bytes)
                              (babel:octets-to-string bytes :encoding :utf-8))))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "enable.idempotence" "true")
                     :serde (lambda (string)
                              (babel:string-to-octets string :encoding :utf-8))))
          (good-partition 1)
          (bad-partition 0)
          (messages '("Here" "are" "some" "messages")))
      (is (string= topic (kf::create-topic producer topic :partitions 2)))
      (sleep 2)
      (kf:subscribe consumer (list topic))
      (sleep 5)

      (mapcar (lambda (message)
                (kf:send producer topic message :partition good-partition))
              messages)
      (mapcar (lambda (message)
                (kf:send producer topic message :partition bad-partition))
              '("These" "messages" "won't" "be" "consumed"))
      (kf:flush producer)

      (kf:pause consumer (list (cons topic bad-partition)))

      (is (equal messages
                 (loop
                    for message = (kf:poll consumer 5000)
                    while message
                    collect (kf:value message)
                    do (kf:commit consumer)))))))

(test consumer-resume
  (with-topics ((topic "consumer-resume-topic" t))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "consumer-resume-group"
                                 "auto.offset.reset" "earliest"
                                 "enable.auto.commit" "false")
                     :serde (lambda (bytes)
                              (babel:octets-to-string bytes :encoding :utf-8))))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "enable.idempotence" "true")
                     :serde (lambda (string)
                              (babel:string-to-octets string :encoding :utf-8))))
          (good-partition 1)
          (bad-partition 0)
          (goodies '("Here" "are" "some" "messages"))
          (baddies '("Here" "are" "some" "more" "messages")))
      (is (string= topic (kf::create-topic producer topic :partitions 2)))
      (sleep 2)
      (kf:subscribe consumer (list topic))
      (sleep 5)

      (mapcar (lambda (message)
                (kf:send producer topic message :partition good-partition))
              goodies)
      (mapcar (lambda (message)
                (kf:send producer topic message :partition bad-partition))
              baddies)
      (kf:flush producer)

      (kf:pause consumer (list (cons topic bad-partition)))
      (is (equal goodies
                 (loop
                    for message = (kf:poll consumer 5000)
                    while message
                    collect (kf:value message)
                    do (kf:commit consumer))))

      (kf:resume consumer (list (cons topic bad-partition)))
      (is (equal baddies
                 (loop
                    for message = (kf:poll consumer 5000)
                    while message
                    collect (kf:value message)
                    do (kf:commit consumer)))))))

(test watermarks
  (with-topics ((topic "watermarks-topic" t))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*)))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "enable.idempotence" "true"))))
      (is (string= topic (kf::create-topic producer topic :partitions 2)))
      (sleep 2)

      (is (equal '(0 . 0) (kf:watermarks consumer topic 0 5000)))
      (is (equal '(0 . 0) (kf:watermarks consumer topic 1 5000)))

      (kf:send producer topic #(2 4) :partition 0)
      (kf:send producer topic #(1 2) :partition 1)
      (kf:send producer topic #(3 4) :partition 1)
      (kf:flush producer)

      (is (equal '(0 . 1) (kf:watermarks consumer topic 0 5000)))
      (is (equal '(0 . 2) (kf:watermarks consumer topic 1 5000))))))

(test offsets-for-times
  (with-topics ((topic "offsets-for-times-topic"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "offsets-for-times-group"
                                 "auto.offset.reset" "earliest"
                                 "enable.auto.commit" "false")
                     :serde (lambda (bytes)
                              (babel:octets-to-string bytes :encoding :utf-8))))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "enable.idempotence" "true")
                     :serde (lambda (string)
                              (babel:string-to-octets string :encoding :utf-8)))))
      (kf:subscribe consumer (list topic))
      (sleep 5)

      (mapcar (lambda (message)
                (kf:send producer topic message)
                (sleep 1))
              '("There" "is" "a" "delay" "between" "these" "messages"))
      (kf:flush producer)

      (let (delay-offset
            delay-timestamp)
        (loop
           for message = (kf:poll consumer 5000)
           while message
           for delay-message-p = (string= (kf:value message) "delay")

           when delay-message-p
           do (setf delay-offset (kf:offset message)
                    delay-timestamp (kf:timestamp message))

           until delay-message-p)

        (is (= delay-offset
               (cdr (assoc (cons topic 0)
                           (kf:offsets-for-times
                            consumer
                            `(((,topic . 0) . ,delay-timestamp))
                            5000)
                           :test #'equal))))
        (is (= (1+ delay-offset)
               (cdr (assoc (cons topic 0)
                           (kf:offsets-for-times
                            consumer
                            `(((,topic . 0) . ,(1+ delay-timestamp)))
                            5000)
                           :test #'equal))))))))

(test positions
  (with-topics ((topic "positions-topic"))
    (let ((consumer (make-instance
                     'kf:consumer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "group.id" "positions-group"
                                 "auto.offset.reset" "earliest"
                                 "enable.auto.commit" "false")
                     :serde (lambda (bytes)
                              (babel:octets-to-string bytes :encoding :utf-8))))
          (producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "enable.idempotence" "true")
                     :serde (lambda (string)
                              (babel:string-to-octets string :encoding :utf-8)))))
      (kf:subscribe consumer (list topic))
      (sleep 5)

      (mapcar (lambda (message)
                (kf:send producer topic message))
              '("Here" "are" "a" "few" "messages"))
      (kf:flush producer)

      (loop
         for i from 0
         for message = (kf:poll consumer 5000)
         while message

         do
           (is (= (1+ i)
                  (cdr (assoc (cons topic 0)
                              (kf:positions consumer (list (cons topic 0)))
                              :test #'equal))))
           (kf:commit consumer)))))
