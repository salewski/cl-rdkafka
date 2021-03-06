;;; Copyright (C) 2018-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(in-package #:cl-rdkafka)

(define-condition kafka-error (error)
  ((description
    :initarg :description
    :initform (error "Must supply description")
    :reader description
    :type string
    :documentation
    "Hopefully some descriptive description describing the error."))
  (:report
   (lambda (condition stream)
     (format stream (description condition))))
  (:documentation
   "Generic condition signalled by cl-rdkafka for expected errors."))

(define-condition rdkafka-error (kafka-error)
  ((enum
    :initarg :enum
    :initform (error "Must supply enum")
    :reader enum
    :type symbol
    :documentation "cl-rdkafka/low-level:rd-kafka-resp-err enum symbol."))
  (:report
   (lambda (condition stream)
     (format stream "librdkafka error ~S: ~S"
             (enum condition)
             (description condition))))
  (:documentation
   "Condition signalled for librdkafka errors."))

(defun make-rdkafka-error (err)
  (declare (symbol err))
  (make-condition 'rdkafka-error
                  :enum err
                  :description (cl-rdkafka/ll:rd-kafka-err2str
                                (symbol-value err))))

(define-condition partition-error (rdkafka-error)
  ((topic
    :initarg :topic
    :initform (error "Must supply topic")
    :reader topic
    :type string
    :documentation "Topic name.")
   (partition
    :initarg :partition
    :initform (error "Must supply partition")
    :reader partition
    :type integer
    :documentation "Topic partition."))
  (:report
   (lambda (condition stream)
     (format stream "Encountered error ~S for `~A:~A`: ~S"
             (enum condition)
             (topic condition)
             (partition condition)
             (description condition))))
  (:documentation
   "Condition signalled for errors specific to a topic's partition."))

(defun make-partition-error (err topic partition)
  (declare (symbol err)
           (string topic)
           (integer partition))
  (make-condition 'partition-error
                  :enum err
                  :description (cl-rdkafka/ll:rd-kafka-err2str
                                (symbol-value err))
                  :topic topic
                  :partition partition))

(define-condition partial-error (kafka-error)
  ((goodies
    :initarg :goodies
    :initform (error "Must supply goodies")
    :reader goodies
    :type sequence
    :documentation "Successful results.")
   (baddies
    :initarg :baddies
    :initform (error "Must supply baddies")
    :reader baddies
    :type sequence
    :documentation "Unsuccessful results."))
  (:report
   (lambda (condition stream)
     (format stream "~A: ~S" (description condition) (baddies condition))))
  (:documentation
   "Condition signalled for operations that partially failed."))

(define-condition allocation-error (storage-condition)
  ((name
    :initarg :name
    :initform (error "Must supply name")
    :reader name
    :type string
    :documentation
    "Name of the object that failed to be allocated.")
   (description
    :initarg :description
    :initform nil
    :reader description
    :type (or null string)
    :documentation
    "Details about why the allocation may have failed."))
  (:report
   (lambda (condition stream)
     (format stream "Failed to allocate new `~A`~@[: ~S~]"
             (name condition)
             (description condition))))
  (:documentation
   "Condition signalled when librdkafka functions fail to allocate pointers."))
