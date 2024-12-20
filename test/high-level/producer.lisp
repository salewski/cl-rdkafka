;;; Copyright (C) 2018-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
;;; Copyright 2024 Google LLC
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

(defpackage #:test/high-level/producer
  (:use #:cl #:1am #:test))

(in-package #:test/high-level/producer)

(defun parse-kcat (output-lines)
  (flet ((parse (partition-key-value)
           (cdr (uiop:split-string partition-key-value :separator "|"))))
    (loop
       for x in output-lines
       by #'cddr
       collect (parse x))))

(test producer-produce
  (with-topics ((topic "test-producer-produce"))
    (let ((producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "enable.idempotence" "true")
                     :serde (lambda (x)
                              (babel:string-to-octets x :encoding :utf-8))))
          (expected '(("key-1" "Hello") ("key-2" "World") ("key-3" "!"))))
      (loop
         for (k v) in expected
         do (kf:send producer topic v :key k)) ; TODO test partition here, too

      (kf:flush producer)
      (sleep 2)

      (let* ((kcat-output-lines
              (uiop:run-program
               (format nil "timeout 5 kcat -CeO -K '%p|%k|%s~A' -b '~A' -t '~A' || exit 0"
                       #\newline
                       *bootstrap-servers*
                       topic)
               :force-shell t
               :output :lines))
             (actual (parse-kcat kcat-output-lines)))
        (is (equal expected actual))))))

(test producer-futures
  (with-topics ((topic "test-producer-futures"))
    (let ((producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "enable.idempotence" "true")
                     :serde #'babel:string-to-octets))
          (expected '(("key-1" "Hello") ("key-2" "World") ("key-3" "!"))))
      (is (equal expected
                 (mapcar (lambda (future)
                           (let ((message (kf:value future)))
                             (if (typep message 'condition)
                                 (error message)
                                 (list (kf:key message) (kf:value message)))))
                         (mapcar (lambda (pair)
                                   (destructuring-bind (key value) pair
                                     (kf:send producer topic value :key key)))
                                 expected)))))))

(test producer-timestamp
  (with-topics ((topic "test-producer-timestamp"))
    (let ((producer (make-instance
                     'kf:producer
                     :conf (list "bootstrap.servers" *bootstrap-servers*
                                 "enable.idempotence" "true")))
          (expected '(2 4 6)))
      (is (equal expected
                 (mapcar
                  (lambda (future)
                    (let ((message (kf:value future)))
                      (kf:timestamp message)))
                  (mapcar
                   (lambda (timestamp)
                     (kf:send producer topic #(1 2 3) :timestamp timestamp))
                   expected)))))))
