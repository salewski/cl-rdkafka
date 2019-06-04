;;; Copyright (C) 2018-2019 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(in-package #:cl-rdkafka/low-level)

(define-foreign-library librdkafka
  (t (:default "librdkafka")))

(use-foreign-library librdkafka)

(defcfun "rd_kafka_version" :int)

(defcfun "rd_kafka_version_str" :string)

(defcenum rd-kafka-type
  rd-kafka-producer
  rd-kafka-consumer)

(defcenum rd-kafka-timestamp-type
  rd-kafka-timestamp-not-available
  rd-kafka-timestamp-create-time
  rd-kafka-timestamp-log-append-time)

(defcfun "rd_kafka_get_debug_contexts" :string)

(defctype rd-kafka-resp-err :int)
(export 'rd-kafka-resp-err)

(define-foreign-type rd-kafka-resp-err ()
  ((num
    :reader num
    :initarg :num))
  (:actual-type :int)
  (:simple-parser rd-kafka-resp-err))
(export 'num)

(defcstruct rd-kafka-err-desc
  (code rd-kafka-resp-err)
  (name :string)
  (desc :string))

(defcfun "rd_kafka_get_err_descs" :void
  (rd-kafka-err-desc (:pointer (:pointer (:struct rd-kafka-err-desc))))
  (cntp (:pointer size-t)))

(defvar *num->err* (make-hash-table :test #'eq)
  "Hash-table to map enum numbers to rd-kafka-resp-err objects.")

;; populate *num->err* with data returned from rd-kafka-get-err-descs
(macrolet ((dashes (name)
	     `(substitute #\- #\_ ,name))

	   (full-name (name)
	     `(concatenate 'string "RD_KAFKA_RESP_ERR_" ,name))

	   (name->enum (name)
	     `(read-from-string (dashes (full-name ,name))))

	   (fill-table (table num name)
	     `(let ((enum-symbol (name->enum ,name))
		    (err (make-instance 'rd-kafka-resp-err :num ,num)))
		(setf
		 (symbol-value enum-symbol) err
		 (gethash ,num ,table) err)
		(export enum-symbol)
		(proclaim `(special ,enum-symbol)))))

  (with-foreign-objects
      ((desc '(:pointer (:pointer (:struct rd-kafka-err-desc))))
       (count '(:pointer size-t)))

    (rd-kafka-get-err-descs desc count)

    (loop
       with *count = (mem-ref count 'size-t)
       with *desc = (mem-ref desc '(:pointer (:struct rd-kafka-err-desc)))

       for i below *count

       for **desc = (mem-aref *desc '(:struct rd-kafka-err-desc) i)
       for num = (getf **desc 'code)
       for name = (getf **desc 'name)

       when name
       do (fill-table *num->err* num name))))

(defmethod translate-to-foreign (rd-kafka-resp-err (type rd-kafka-resp-err))
  (num rd-kafka-resp-err))

(defmethod translate-from-foreign (num (type rd-kafka-resp-err))
  (multiple-value-bind (err exists-p) (gethash num *num->err*)
    (if exists-p
	err
	(error "Unknown rd-kafka-resp-err number: ~A~%" num))))

(defcfun "rd_kafka_err2str" :string
  (err rd-kafka-resp-err))

(defcfun "rd_kafka_err2name" :string
  (err rd-kafka-resp-err))

(defcfun "rd_kafka_last_error" rd-kafka-resp-err)

(defcfun "rd_kafka_errno2err" rd-kafka-resp-err
  (errnox :int))

(defcfun "rd_kafka_errno" :int)

(defcfun "rd_kafka_fatal_error" rd-kafka-resp-err
  (rk (:pointer rd-kafka-type))
  (errstr :string)
  (errstr-size size-t))

(defcstruct rd-kafka-topic-partition
  (topic :string)
  (partition :int32)
  (offset :int64)
  (metadata (:pointer :void))
  (metadata-size size-t)
  (opaque (:pointer :void))
  (err rd-kafka-resp-err)
  (private (:pointer :void)))

(defcfun "rd_kafka_topic_partition_destroy" :void
  (rktpar (:pointer (:struct rd-kafka-topic-partition))))

(defcstruct rd-kafka-topic-partition-list
  (cnt :int)
  (size :int)
  (elems (:pointer (:struct rd-kafka-topic-partition))))

(defcfun "rd_kafka_topic_partition_list_new"
    (:pointer (:struct rd-kafka-topic-partition-list))
  (size :int))

(defcfun "rd_kafka_topic_partition_list_destroy" :void
  (rkparlist (:pointer (:struct rd-kafka-topic-partition-list))))

(defcfun "rd_kafka_topic_partition_list_add"
    (:pointer (:struct rd-kafka-topic-partition))
  (rktparlist (:pointer (:struct rd-kafka-topic-partition-list)))
  (topic :string)
  (partition :int32))

(defcfun "rd_kafka_topic_partition_list_add_range" :void
  (rktparlist (:pointer (:struct rd-kafka-topic-partition-list)))
  (topic :string)
  (start :int32)
  (stop :int32))

(defcfun "rd_kafka_topic_partition_list_del" :int
  (rktparlist (:pointer (:struct rd-kafka-topic-partition-list)))
  (topic :string)
  (partition :int32))

(defcfun "rd_kafka_topic_partition_list_del_by_idx" :int
  (rktparlist (:pointer (:struct rd-kafka-topic-partition-list)))
  (idx :int))

(defcfun "rd_kafka_topic_partition_list_copy"
    (:pointer (:struct rd-kafka-topic-partition-list))
  (src (:pointer (:struct rd-kafka-topic-partition-list))))

(defcfun "rd_kafka_topic_partition_list_set_offset" rd-kafka-resp-err
  (rktparlist (:pointer (:struct rd-kafka-topic-partition-list)))
  (topic :string)
  (partition :int32)
  (offset :int64))

(defcfun "rd_kafka_topic_partition_list_find"
    (:pointer (:struct rd-kafka-topic-partition))
  (rktparlist (:pointer (:struct rd-kafka-topic-partition-list)))
  (topic :string)
  (partition :int32))

(defcfun "rd_kafka_topic_partition_list_sort" :void
  (rktparlist (:pointer (:struct rd-kafka-topic-partition-list)))
  (cmp (:pointer :void))
  (opaque (:pointer :void)))

(defcenum rd-kafka-vtype
  rd-kafka-vtype-end
  rd-kafka-vtype-topic
  rd-kafka-vtype-rkt
  rd-kafka-vtype-partition
  rd-kafka-vtype-value
  rd-kafka-vtype-key
  rd-kafka-vtype-opaque
  rd-kafka-vtype-msgflags
  rd-kafka-vtype-timestamp
  rd-kafka-vtype-header
  rd-kafka-vtype-headers)

;; because rd_kafka_headers_t is an opaque struct
(defctype rd-kafka-headers :void)

(defcfun "rd_kafka_headers_new" (:pointer rd-kafka-headers)
  (initial-count size-t))

(defcfun "rd_kafka_headers_destroy" :void
  (hdrs (:pointer rd-kafka-headers)))

(defcfun "rd_kafka_headers_copy" (:pointer rd-kafka-headers)
  (src (:pointer rd-kafka-headers)))

(defcfun "rd_kafka_header_add" rd-kafka-resp-err
  (hdrs (:pointer rd-kafka-headers))
  (name :string)
  (name-size ssize-t)
  (value (:pointer :void))
  (value-size ssize-t))

(defcfun "rd_kafka_header_remove" rd-kafka-resp-err
  (hdrs (:pointer rd-kafka-headers))
  (name :string))

(defcfun "rd_kafka_header_get_last" rd-kafka-resp-err
  (hdrs (:pointer rd-kafka-headers))
  (name :string)
  (valuep (:pointer (:pointer :void)))
  (sizep (:pointer size-t)))

(defcfun "rd_kafka_header_get" rd-kafka-resp-err
  (hdrs (:pointer rd-kafka-headers))
  (idx size-t)
  (name :string)
  (valuep (:pointer (:pointer :void)))
  (sizep (:pointer size-t)))

(defcfun "rd_kafka_header_get_all" rd-kafka-resp-err
  (hdrs (:pointer rd-kafka-headers))
  (idx size-t)
  (namep (:pointer :string))
  (valuep (:pointer (:pointer :void)))
  (sizep (:pointer size-t)))

;; because rd_kafka_topic_t is an opaque struct
(defctype rd-kafka-topic :void)

(defcstruct rd-kafka-message
  (err rd-kafka-resp-err)
  (rkt (:pointer rd-kafka-topic))
  (partition :int32)
  (payload (:pointer :void))
  (len size-t)
  (key (:pointer :void))
  (key-len size-t)
  (offset :int64)
  (private (:pointer :void)))

(defcfun "rd_kafka_message_destroy" :void
  (rkmessage (:pointer (:struct rd-kafka-message))))

(defcfun "rd_kafka_message_timestamp" :int64
  (rkmessage (:pointer (:struct rd-kafka-message)))
  (tstype (:pointer rd-kafka-timestamp-type)))

(defcfun "rd_kafka_message_latency" :int64
  (rkmessage (:pointer (:struct rd-kafka-message))))

(defcfun "rd_kafka_message_headers" rd-kafka-resp-err
  (rkmessage (:pointer (:struct rd-kafka-message)))
  (hdrsp (:pointer (:pointer rd-kafka-headers))))

(defcfun "rd_kafka_message_detach_headers" rd-kafka-resp-err
  (rkmessage (:pointer (:struct rd-kafka-message)))
  (hdrsp (:pointer (:pointer rd-kafka-headers))))

(defcfun "rd_kafka_message_set_headers" :void
  (rkmessage (:pointer (:struct rd-kafka-message)))
  (hdrs (:pointer rd-kafka-headers)))

(defcfun "rd_kafka_header_cnt" size-t
  (hdrs (:pointer rd-kafka-headers)))

(defcenum rd-kafka-msg-status
  (rd-kafka-msg-status-not-persisted 0)
  (rd-kafka-msg-status-possibly-persisted 1)
  (rd-kafka-msg-status-persisted 2))

(defcfun "rd_kafka_message_status" rd-kafka-msg-status
  (rkmessage (:pointer (:struct rd-kafka-message))))

(defcenum rd-kafka-conf-res
  (rd-kafka-conf-unknown -2)
  (rd-kafka-conf-invalid -1)
  (rd-kafka-conf-ok 0))

;; because rd_kafka_conf_t is an opaque struct
(defctype rd-kafka-conf :void)

(defcfun "rd_kafka_conf_new" (:pointer rd-kafka-conf))

(defcfun "rd_kafka_conf_destroy" :void
  (conf (:pointer rd-kafka-conf)))

(defcfun "rd_kafka_conf_dup" (:pointer rd-kafka-conf)
  (conf (:pointer rd-kafka-conf)))

(defcfun "rd_kafka_conf_dup_filter" (:pointer rd-kafka-conf)
  (conf (:pointer rd-kafka-conf))
  (filter-cnt size-t)
  (filter (:pointer :string)))

(defcfun "rd_kafka_conf_set" rd-kafka-conf-res
  (conf (:pointer rd-kafka-conf))
  (name :string)
  (value :string)
  (errstr :string)
  (errstr-size size-t))

(defcfun "rd_kafka_conf_set_events" :void
  (conf (:pointer rd-kafka-conf))
  (events :int))

(defcfun "rd_kafka_conf_set_background_event_cb" :void
  (conf (:pointer rd-kafka-conf))
  (event-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_dr_cb" :void
  (conf (:pointer rd-kafka-conf))
  (dr-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_dr_msg_cb" :void
  (conf (:pointer rd-kafka-conf))
  (dr-msg-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_consume_cb" :void
  (conf (:pointer rd-kafka-conf))
  (consume-db (:pointer :void)))

(defcfun "rd_kafka_conf_set_rebalance_cb" :void
  (conf (:pointer rd-kafka-conf))
  (rebalance-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_offset_commit_cb" :void
  (conf (:pointer rd-kafka-conf))
  (offset-commit-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_error_cb" :void
  (conf (:pointer rd-kafka-conf))
  (error-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_throttle_cb" :void
  (conf (:pointer rd-kafka-conf))
  (throttle-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_log_cb" :void
  (conf (:pointer rd-kafka-conf))
  (log-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_stats_cb" :void
  (conf (:pointer rd-kafka-conf))
  (stats-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_socket_cb" :void
  (conf (:pointer rd-kafka-conf))
  (socket-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_connect_cb" :void
  (conf (:pointer rd-kafka-conf))
  (connect-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_closesocket_cb" :void
  (conf (:pointer rd-kafka-conf))
  (closesocker-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_open_cb" :void
  (conf (:pointer rd-kafka-conf))
  (open-cb (:pointer :void)))

(defcfun "rd_kafka_conf_set_opaque" :void
  (conf (:pointer rd-kafka-conf))
  (opaque (:pointer :void)))

(defcfun "rd_kafka_opaque" (:pointer :void)
  (rk (:pointer rd-kafka-type)))

;; because rd_kafka_topic_conf_t is an opaque struct
(defctype rd-kafka-topic-conf :void)

(defcfun "rd_kafka_conf_set_default_topic_conf" :void
  (conf (:pointer rd-kafka-conf))
  (tconf (:pointer rd-kafka-topic-conf)))

(defcfun "rd_kafka_conf_get" rd-kafka-conf-res
  (conf (:pointer rd-kafka-conf))
  (name :string)
  (dest :string)
  (dest-size (:pointer size-t)))

(defcfun "rd_kafka_topic_conf_get" rd-kafka-conf-res
  (conf (:pointer rd-kafka-topic-conf))
  (name :string)
  (dest :string)
  (dest-size (:pointer size-t)))

(defcfun "rd_kafka_conf_dump" (:pointer :string)
  (conf (:pointer rd-kafka-conf))
  (cntp (:pointer size-t)))

(defcfun "rd_kafka_topic_conf_dump" (:pointer :string)
  (conf (:pointer rd-kafka-topic-conf))
  (cntp (:pointer size-t)))

(defcfun "rd_kafka_conf_dump_free" :void
  (arr (:pointer :string))
  (cnt size-t))

(defcfun "rd_kafka_conf_properties_show" :void
  (fp (:pointer :void)))

(defcfun "rd_kafka_topic_conf_new" (:pointer rd-kafka-topic-conf))

(defcfun "rd_kafka_topic_conf_dup" (:pointer rd-kafka-topic-conf)
  (conf (:pointer rd-kafka-topic-conf)))

(defcfun "rd_kafka_default_topic_conf_dup" (:pointer rd-kafka-topic-conf)
  (rk (:pointer rd-kafka-type)))

(defcfun "rd_kafka_topic_conf_destroy" :void
  (topic-conf (:pointer rd-kafka-topic-conf)))

(defcfun "rd_kafka_topic_conf_set" rd-kafka-conf-res
  (conf (:pointer rd-kafka-topic-conf))
  (name :string)
  (value :string)
  (errstr :string)
  (errstr-size size-t))

(defcfun "rd_kafka_topic_conf_set_opaque" :void
  (conf (:pointer rd-kafka-topic-conf))
  (opaque (:pointer :void)))

(defcfun "rd_kafka_topic_conf_set_partitioner_cb" :void
  (topic-conf (:pointer rd-kafka-topic-conf))
  (partitioner (:pointer :void)))

(defcfun "rd_kafka_topic_conf_set_msg_order_cmp" :void
  (topic-conf (:pointer rd-kafka-topic-conf))
  (msg-order-cmp (:pointer :void)))

(defcfun "rd_kafka_topic_partition_available" :void
  (rkt (:pointer rd-kafka-topic))
  (partition :int32))

(defcfun "rd_kafka_msg_partitioner_random" :int32
  (rkt (:pointer rd-kafka-topic))
  (key (:pointer :void))
  (keylen size-t)
  (partition-cnt :int32)
  (opaque (:pointer :void))
  (msg-options (:pointer :void)))

(defcfun "rd_kafka_msg_partitioner_consistent" :int32
  (rkt (:pointer rd-kafka-topic))
  (key (:pointer :void))
  (keylen size-t)
  (partition-cnt :int32)
  (opaque (:pointer :void))
  (msg-options (:pointer :void)))

(defcfun "rd_kafka_msg_partitioner_consistent_random" :int32
  (rkt (:pointer rd-kafka-topic))
  (key (:pointer :void))
  (keylen size-t)
  (partition-cnt :int32)
  (opaque (:pointer :void))
  (msg-opaque (:pointer :void)))

(defcfun "rd_kafka_msg_partitioner_murmur2" :int32
  (rkt (:pointer rd-kafka-topic))
  (key (:pointer :void))
  (keylen size-t)
  (partition-cnt :int32)
  (rkt-opaque (:pointer :void))
  (msg-opaque (:pointer :void)))

(defcfun "rd_kafka_msg_partitioner_murmur2_random" :int32
  (rkt (:pointer rd-kafka-topic))
  (key (:pointer :void))
  (keylen size-t)
  (partition-cnt :int32)
  (rkt-opaque (:pointer :void))
  (msg-options (:pointer :void)))

(defcfun "rd_kafka_new" (:pointer rd-kafka-type)
  (type rd-kafka-type)
  (conf (:pointer rd-kafka-conf))
  (errstr :string)
  (errstr-size size-t))

(defcfun "rd_kafka_destroy" :void
  (rk (:pointer rd-kafka-type)))

(defcfun "rd_kafka_destroy_flags" :void
  (rk (:pointer rd-kafka-type))
  (flags :int))

(defcfun "rd_kafka_name" :string
  (rk (:pointer rd-kafka-type)))

;; TODO pointer to rd-kafka-type should be a (:pointer :rd-kafka)
;; where rd-kafka defctypes to :void
;; this is because rd-kafka-type is an enum and rd-kafka is an opaque struct
(defcfun "rd_kafka_type" rd-kafka-type
  (rk (:pointer rd-kafka-type)))

(defcfun "rd_kafka_memberid" :string
  (rk :pointer))

(defcfun "rd_kafka_clusterid" :string
  (rk :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_controllerid" :int32
  (rk :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_topic_new" :pointer
  (rk :pointer)
  (topic :string)
  (conf :pointer))

(defcfun "rd_kafka_topic_destroy" :void
  (rkt :pointer))

(defcfun "rd_kafka_topic_name" :string
  (rkt :pointer))

(defcfun "rd_kafka_topic_opaque" :pointer
  (rtk :pointer))

(defcfun "rd_kafka_poll" :int
  (rk :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_yield" :void
  (rk :pointer))

(defcfun "rd_kafka_pause_partitions" rd-kafka-resp-err
  (rk :pointer)
  (partitions :pointer))

(defcfun "rd_kafka_resume_partitions" rd-kafka-resp-err
  (rk :pointer)
  (partitions :pointer))

(defcfun "rd_kafka_query_watermark_offsets" rd-kafka-resp-err
  (rk :pointer)
  (topic :string)
  (partitoin :int32)
  (low :pointer)
  (high :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_get_watermark_offsets" rd-kafka-resp-err
  (rk :pointer)
  (topic :string)
  (partition :int32)
  (low :pointer)
  (high :pointer))

(defcfun "rd_kafka_offsets_for_times" rd-kafka-resp-err
  (rk :pointer)
  (offsets :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_mem_free" :void
  (rk :pointer)
  (ptr :pointer))

(defcfun "rd_kafka_queue_new" :pointer
  (rk :pointer))

(defcfun "rd_kafka_queue_destroy" :void
  (rkqu :pointer))

(defcfun "rd_kafka_queue_get_main" :pointer
  (rk :pointer))

(defcfun "rd_kafka_queue_get_consumer" :pointer
  (rk :pointer))

(defcfun "rd_kafka_queue_get_partition" :pointer
  (rk :pointer)
  (topic :string)
  (partition :int32))

(defcfun "rd_kafka_queue_get_background" :pointer
  (rk :pointer))

(defcfun "rd_kafka_queue_forward" :void
  (src :pointer)
  (dst :pointer))

(defcfun "rd_kafka_set_log_queue" rd-kafka-resp-err
  (rk :pointer)
  (rkqu :pointer))

(defcfun "rd_kafka_queue_length" size-t
  (rkqu :pointer))

(defcfun "rd_kafka_queue_io_event_enable" :void
  (rkqu :pointer)
  (fd :int)
  (payload :pointer)
  (size size-t))

(defcfun "rd_kafka_queue_cb_event_enable" :void
  (rkqu :pointer)
  (event-cb :pointer)
  (opaque :pointer))

(defcfun "rd_kafka_consume_start" :int
  (rkt :pointer)
  (partition :int32)
  (offset :int64))

(defcfun "rd_kafka_consume_start_queue" :int
  (rkt :pointer)
  (partition :int32)
  (offset :int64)
  (rkqu :pointer))

(defcfun "rd_kafka_consume_stop" :int
  (rkt :pointer)
  (partition :int32))

(defcfun "rd_kafka_seek" rd-kafka-resp-err
  (rkt :pointer)
  (partition :int32)
  (offset :int64)
  (timeout-ms :int))

(defcfun "rd_kafka_consume" :pointer
  (rkt :pointer)
  (partition :int32)
  (timeout-ms :int))

(defcfun "rd_kafka_consume_batch" ssize-t
  (rkt :pointer)
  (partition :int32)
  (timeout-ms :int)
  (rkmessages :pointer)
  (rkmessages-size size-t))

(defcfun "rd_kafka_consume_callback" :int
  (rkt :pointer)
  (partition :int32)
  (timeout-ms :int)
  (consume-cb :pointer)
  (opaque :pointer))

(defcfun "rd_kafka_consume_queue" :pointer
  (rkqu :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_consume_batch_queue" ssize-t
  (rkqu :pointer)
  (timeout-ms :int)
  (rkmessages :pointer)
  (rkmessages-size size-t))

(defcfun "rd_kafka_consume_callback_queue" :int
  (rkqu :pointer)
  (timeout-ms :int)
  (consume-cb :pointer)
  (opaque :pointer))

(defcfun "rd_kafka_offset_store" rd-kafka-resp-err
  (rkt :pointer)
  (partition :int32)
  (offset :int64))

(defcfun "rd_kafka_offsets_store" rd-kafka-resp-err
  (rk :pointer)
  (offsets :pointer))

(defcfun "rd_kafka_subscribe" rd-kafka-resp-err
  (rk :pointer)
  (topics :pointer))

(defcfun "rd_kafka_unsubscribe" rd-kafka-resp-err
  (rk :pointer))

(defcfun "rd_kafka_subscription" rd-kafka-resp-err
  (rk :pointer)
  (topics :pointer))

(defcfun "rd_kafka_consumer_poll" :pointer
  (rk :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_consumer_close" rd-kafka-resp-err
  (rk :pointer))

(defcfun "rd_kafka_assign" rd-kafka-resp-err
  (rk :pointer)
  (partitions :pointer))

(defcfun "rd_kafka_assignment" rd-kafka-resp-err
  (rk :pointer)
  (partitions :pointer))

(defcfun "rd_kafka_commit" rd-kafka-resp-err
  (rk :pointer)
  (offsets :pointer)
  (async :int))

(defcfun "rd_kafka_commit_message" rd-kafka-resp-err
  (rk :pointer)
  (rkmessage :pointer)
  (async :int))

(defcfun "rd_kafka_commit_queue" rd-kafka-resp-err
  (rk :pointer)
  (offsets :pointer)
  (rkqu :pointer)
  (cb :pointer)
  (opaque :pointer))

(defcfun "rd_kafka_committed" rd-kafka-resp-err
  (rk :pointer)
  (partitions :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_position" rd-kafka-resp-err
  (rk :pointer)
  (partitions :pointer))

(defcfun "rd_kafka_produce" :int
  (rkt :pointer)
  (partition :int32)
  (msgflags :int)
  (payload :pointer)
  (len size-t)
  (key :pointer)
  (keylen size-t)
  (msg-opaque :pointer))

;; read defcfun docs about variadic funcs returning structs by value.
;; I think I just need to load cffi-libffi along with libffi-dev
(defcfun "rd_kafka_producev" rd-kafka-resp-err
  (rk :pointer)
  &rest)

(defcfun "rd_kafka_produce_batch" :int
  (rkt :pointer)
  (partition :int32)
  (msgflags :int)
  (rkmessages :pointer)
  (message-cnt :int))

(defcfun "rd_kafka_flush" rd-kafka-resp-err
  (rk :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_purge" rd-kafka-resp-err
  (rk :pointer)
  (purge-flags :int))

(defcstruct rd-kafka-metadata-broker
  (id :int32)
  (host :string)
  (port :int))

(defcstruct rd-kafka-metadata-partition
  (id :int32)
  (err rd-kafka-resp-err)
  (leader :int32)
  (replica-cnt :int)
  (replicas :pointer)
  (isr-cnt :int)
  (isrs :pointer))

(defcstruct rd-kafka-metadata-topic
  (topic :string)
  (partition-cnt :int)
  (partitions :pointer)
  (err rd-kafka-resp-err))

(defcstruct rd-kafka-metadata
  (broker-cnt :int)
  (brokers :pointer)
  (topic-cnt :int)
  (topics :pointer)
  (orig-broker-id :int32)
  (orig-broker-name :string))

(defcfun "rd_kafka_metadata" rd-kafka-resp-err
  (rk :pointer)
  (all-topics :int)
  (only-rkt :pointer)
  (metadatap :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_metadata_destroy" :void
  (metadata :pointer))

(defcstruct rd-kafka-group-member-info
  (member-id :string)
  (client-id :string)
  (client-host :string)
  (member-metadata :pointer)
  (member-metadata-size :int)
  (member-assignment :pointer)
  (member-assignment-size :int))

(defcstruct rd-kafka-group-info
  (broker (:struct rd-kafka-metadata-broker))
  (group :string)
  (err rd-kafka-resp-err)
  (state :string)
  (protocol-type :string)
  (protocol :string)
  (members :pointer)
  (member-cnt :int))

(defcstruct rd-kafka-group-list
  (groups :pointer)
  (group-cnt :int))

(defcfun "rd_kafka_list_groups" rd-kafka-resp-err
  (rk :pointer)
  (group :string)
  (grplistp :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_group_list_destroy" :void
  (grplist :pointer))

(defcfun "rd_kafka_brokers_add" :int
  (rk :pointer)
  (brokerlist :string))

(defcfun "rd_kafka_set_logger" :void
  (rk :pointer)
  (func :pointer))

(defcfun "rd_kafka_set_log_level" :void
  (rk :pointer)
  (level :int))

(defcfun "rd_kafka_log_print" :void
  (rk :pointer)
  (level :int)
  (fac :string)
  (buf :string))

(defcfun "rd_kafka_log_syslog" :void
  (rk :pointer)
  (level :int)
  (fac :string)
  (buf :string))

(defcfun "rd_kafka_outq_len" :int
  (rk :pointer))

(defcfun "rd_kafka_dump" :void
  (rp :pointer)
  (rk :pointer))

(defcfun "rd_kafka_thread_cnt" :int)

(defcfun "rd_kafka_wait_destroyed" :int
  (timeout-ms :int))

(defcfun "rd_kafka_unittest" :int)

(defcfun "rd_kafka_poll_set_consumer" rd-kafka-resp-err
  (rk :pointer))

(defctype rd-kafka-event-type :int)

(defcfun "rd_kafka_event_type" rd-kafka-event-type
  (rkev :pointer))

(defcfun "rd_kafka_event_name" :string
  (rkev :pointer))

(defcfun "rd_kafka_event_destroy" :void
  (rkev :pointer))

(defcfun "rd_kafka_event_message_next" :pointer
  (rkev :pointer))

(defcfun "rd_kafka_event_message_array" size-t
  (rkev :pointer)
  (rkmessages :pointer)
  (size size-t))

(defcfun "rd_kafka_event_message_count" size-t
  (rkev :pointer))

(defcfun "rd_kafka_event_error" rd-kafka-resp-err
  (rkev :pointer))

(defcfun "rd_kafka_event_error_string" :string
  (rkev :pointer))

(defcfun "rd_kafka_event_error_is_fatal" :int
  (rkev :pointer))

(defcfun "rd_kafka_event_opaque" :pointer
  (rkev :pointer))

(defcfun "rd_kafka_event_log" :int
  (rkev :pointer)
  (fac :pointer)
  (str :pointer)
  (level :int))

(defcfun "rd_kafka_event_stats" :string
  (rkev :pointer))

(defcfun "rd_kafka_event_topic_partition_list" :pointer
  (rkev :pointer))

(defcfun "rd_kafka_event_topic_partition" :pointer
  (rkev :pointer))

(defctype rd-kafka-create-topics-result rd-kafka-event-type)
(defctype rd-kafka-delelte-topics-result rd-kafka-event-type)
(defctype rd-kafka-create-partitions-result rd-kafka-event-type)
(defctype rd-kafka-alter-configs-result rd-kafka-event-type)
(defctype rd-kafka-describe-configs-result rd-kafka-event-type)

(defcfun "rd_kafka_event_CreateTopics_result" :pointer
  (rkev :pointer))

(defcfun "rd_kafka_event_DeleteTopics_result" :pointer
  (rkev :pointer))

(defcfun "rd_kafka_event_CreatePartitions_result" :pointer
  (rkev :pointer))

(defcfun "rd_kafka_event_AlterConfigs_result" :pointer
  (rkev :pointer))

(defcfun "rd_kafka_event_DescribeConfigs_result" :pointer
  (rkev :pointer))

(defcfun "rd_kafka_queue_poll" :pointer
  (rkqu :pointer)
  (timeout-ms :int))

(defcfun "rd_kafka_queue_poll_callback" :int
  (rkqu :pointer)
  (timeout-ms :int))

(defctype rd-kafka-plugin-f-conf-init rd-kafka-resp-err)

(defctype rd-kafka-interceptor-f-on-conf-set rd-kafka-conf-res)

(defctype rd-kafka-interceptor-f-on-conf-dup rd-kafka-resp-err)

(defctype rd-kafka-interceptor-f-on-conf-destroy rd-kafka-resp-err)

(defctype rd-kafka-interceptor-f-on-new rd-kafka-resp-err)

(defctype rd-kafka-interceptor-f-on-destroy rd-kafka-resp-err)

(defctype rd-kafka-interceptor-f-on-send rd-kafka-resp-err)

(defctype rd-kafka-interceptor-f-on-acknowledgement rd-kafka-resp-err)

(defctype rd-kafka-interceptor-f-on-consume rd-kafka-resp-err)

(defctype rd-kafka-interceptor-f-on-commit rd-kafka-resp-err)

(defctype rd-kafka-interceptor-f-on-request-sent rd-kafka-resp-err)

(defcfun "rd_kafka_conf_interceptor_add_on_conf_set" rd-kafka-resp-err
  (conf :pointer)
  (ic-name :string)
  (on-conf-set :pointer)
  (ic-opaque :pointer))

(defcfun "rd_kafka_conf_interceptor_add_on_conf_dup" rd-kafka-resp-err
  (conf :pointer)
  (ic-name :string)
  (on-conf-dup :pointer)
  (ic-opaque :pointer))

(defcfun "rd_kafka_conf_interceptor_add_on_conf_destroy" rd-kafka-resp-err
  (conf :pointer)
  (ic-name :string)
  (on-conf-destroy :pointer)
  (ic-opaque :pointer))

(defcfun "rd_kafka_conf_interceptor_add_on_new" rd-kafka-resp-err
  (conf :pointer)
  (ic-name :string)
  (on-new :pointer)
  (ic-opaque :pointer))

(defcfun "rd_kafka_interceptor_add_on_destroy" rd-kafka-resp-err
  (rk :pointer)
  (ic-name :string)
  (on-destroy :pointer)
  (ic-opaque :pointer))

(defcfun "rd_kafka_interceptor_add_on_send" rd-kafka-resp-err
  (rk :pointer)
  (ic-name :string)
  (on-send :pointer)
  (ic-opaque :pointer))

(defcfun "rd_kafka_interceptor_add_on_acknowledgement" rd-kafka-resp-err
  (rk :pointer)
  (ic-name :string)
  (on-acknowledgement :pointer)
  (ic-opaque :pointer))

(defcfun "rd_kafka_interceptor_add_on_consume" rd-kafka-resp-err
  (rk :pointer)
  (ic-name :string)
  (on-consume :pointer)
  (ic-opaque :pointer))

(defcfun "rd_kafka_interceptor_add_on_commit" rd-kafka-resp-err
  (rk :pointer)
  (ic-name :string)
  (on-commit :pointer)
  (ic-opaque :pointer))

(defcfun "rd_kafka_interceptor_add_on_request_sent" rd-kafka-resp-err
  (rk :pointer)
  (ic-name :string)
  (on-request-sent :pointer)
  (ic-opaque :pointer))

(defcfun "rd_kafka_topic_result_error" rd-kafka-resp-err
  (topicres :pointer))

(defcfun "rd_kafka_topic_result_error_string" :string
  (topicres :pointer))

(defcfun "rd_kafka_topic_result_name" :string
  (topicres :pointer))

(defcenum rd-kafka-admin-op
  (rd-kafka-admin-op-any 0)
  rd-kafka-admin-op-createtopics
  rd-kafka-admin-op-deletetopics
  rd-kafka-admin-op-createpartitions
  rd-kafka-admin-op-alterconfigs
  rd-kafka-admin-op-describeconfigs
  rd-kafka-admin-op--cnt)

(defcfun "rd_kafka_AdminOptions_new" :pointer
  (rk :pointer)
  (for-api rd-kafka-admin-op))

(defcfun "rd_kafka_AdminOptions_destroy" :void
  (options :pointer))

(defcfun "rd_kafka_AdminOptions_set_request_timeout" rd-kafka-resp-err
  (options :pointer)
  (timeout-ms :int)
  (errstr :string)
  (errstr-size size-t))

(defcfun "rd_kafka_AdminOptions_set_operation_timeout" rd-kafka-resp-err
  (options :pointer)
  (timeout-ms :int)
  (errstr :string)
  (errstr-size size-t))

(defcfun "rd_kafka_AdminOptions_set_validate_only" rd-kafka-resp-err
  (options :pointer)
  (true-or-false :int)
  (errstr :string)
  (errstr-size size-t))

(defcfun "rd_kafka_AdminOptions_set_broker" rd-kafka-resp-err
  (options :pointer)
  (broker-id :int32)
  (errstr :string)
  (errstr-size size-t))

(defcfun "rd_kafka_AdminOptions_set_opaque" :void
  (options :pointer)
  (opaque :pointer))

(defcfun "rd_kafka_NewTopic_new" :pointer
  (topic :string)
  (num-partitions :int)
  (replication-factor :int)
  (errstr :string)
  (errstr-size size-t))

(defcfun "rd_kafka_NewTopic_destroy" :void
  (new-topic :pointer))

(defcfun "rd_kafka_NewTopic_destroy_array" :void
  (new-topics :pointer)
  (new-topic-cnt size-t))

(defcfun "rd_kafka_NewTopic_set_replica_assignment" rd-kafka-resp-err
  (new-topic :pointer)
  (partition :int32)
  (broker-ids :pointer)
  (broker-id-cnt size-t)
  (errstr :string)
  (errstr-size size-t))

(defcfun "rd_kafka_NewTopic_set_config" rd-kafka-resp-err
  (new-topic :pointer)
  (name :string)
  (value :string))

(defcfun "rd_kafka_CreateTopics" :void
  (rk :pointer)
  (new-topics :pointer)
  (new-topic-cnt size-t)
  (options :pointer)
  (rkqu :pointer))

(defcfun "rd_kafka_CreateTopics_result_topics" :pointer
  (result :pointer)
  (cntp :pointer))

(defcfun "rd_kafka_DeleteTopic_new" :pointer
  (topic :string))

(defcfun "rd_kafka_DeleteTopic_destroy" :void
  (del-topic :pointer))

(defcfun "rd_kafka_DeleteTopic_destroy_array" :void
  (del-topics :pointer)
  (del-topic-cnt size-t))

(defcfun "rd_kafka_DeleteTopics" :void
  (rk :pointer)
  (del-topics :pointer)
  (del-topic-cnt size-t)
  (options :pointer)
  (rkqu :pointer))

(defcfun "rd_kafka_DeleteTopics_result_topics" :pointer
  (result :pointer)
  (cntp :pointer))

(defcfun "rd_kafka_NewPartitions_new" :pointer
  (topic :string)
  (new-total-cnt size-t)
  (errstr :pointer)
  (errstr-size size-t))

(defcfun "rd_kafka_NewPartitions_destroy" :void
  (new-parts :pointer))

(defcfun "rd_kafka_NewPartitions_destroy_array" :void
  (new-parts :pointer)
  (new-parts-cnt size-t))

(defcfun "rd_kafka_NewPartitions_set_replica_assignment" rd-kafka-resp-err
  (new-parts :pointer)
  (new-partition-idx :int32)
  (broker-ids :pointer)
  (broker-id-cnt size-t)
  (errstr :string)
  (errstr-size size-t))

(defcfun "rd_kafka_CreatePartitions" :void
  (rk :pointer)
  (new-parts :pointer)
  (new-parts-cnt size-t)
  (options :pointer)
  (rkqu :pointer))

(defcfun "rd_kafka_CreatePartitions_result_topics" :pointer
  (result :pointer)
  (cntp :pointer))

(defcenum rd-kafka-config-source
  (rd-kafka-config-source-unknown-config 0)
  (rd-kafka-config-source-dynamic-topic-config 1)
  (rd-kafka-config-source-dynamic-broker-config 2)
  (rd-kafka-config-source-dynamic-default-broker-config 3)
  (rd-kafka-config-source-static-broker-config 4)
  (rd-kafka-config-source-default-config 5)
  rd-kafka-config-source--cnt)

(defcfun "rd_kafka_ConfigSource_name" :string
  (conf-source rd-kafka-config-source))

(defcfun "rd_kafka_ConfigEntry_name" :string
  (entry :pointer))

(defcfun "rd_kafka_ConfigEntry_value" :string
  (entry :pointer))

(defcfun "rd_kafka_ConfigEntry_source" rd-kafka-config-source
  (entry :pointer))

(defcfun "rd_kafka_ConfigEntry_is_read_only" :int
  (entry :pointer))

(defcfun "rd_kafka_ConfigEntry_is_default" :int
  (entry :pointer))

(defcfun "rd_kafka_ConfigEntry_is_sensitive" :int
  (entry :pointer))

(defcfun "rd_kafka_ConfigEntry_is_synonym" :int
  (entry :pointer))

(defcfun "rd_kafka_ConfigEntry_synonyms" :pointer
  (entry :pointer)
  (cntp :pointer))

(defcenum rd-kafka-resource-type
  (rd-kafka-resource-unknown 0)
  (rd-kafka-resource-any 1)
  (rd-kafka-resource-topic 2)
  (rd-kafka-resource-group 3)
  (rd-kafka-resource-broker 4)
  rd-kafka-resource--cnt)

(defcfun "rd_kafka_ResourceType_name" :string
  (resttype rd-kafka-resource-type))

(defcfun "rd_kafka_ConfigResource_new" :pointer
  (restype rd-kafka-resource-type)
  (resname :string))

(defcfun "rd_kafka_ConfigResource_destroy" :void
  (config :pointer))

(defcfun "rd_kafka_ConfigResource_destroy_array" :void
  (config :pointer)
  (config-cnt size-t))

(defcfun "rd_kafka_ConfigResource_set_config" rd-kafka-resp-err
  (config :pointer)
  (name :string)
  (value :string))

(defcfun "rd_kafka_ConfigResource_configs" :pointer
  (config :pointer)
  (cntp :pointer))

(defcfun "rd_kafka_ConfigResource_type" rd-kafka-resource-type
  (config :pointer))

(defcfun "rd_kafka_ConfigResource_name" :string
  (config :pointer))

(defcfun "rd_kafka_ConfigResource_error" rd-kafka-resp-err
  (config :pointer))

(defcfun "rd_kafka_ConfigResource_error_string" :string
  (config :pointer))

(defcfun "rd_kafka_AlterConfigs" :void
  (rk :pointer)
  (configs :pointer)
  (config-cnt size-t)
  (options :pointer)
  (rkqu :pointer))

(defcfun "rd_kafka_AlterConfigs_result_resources" :pointer
  (result :pointer)
  (cntp :pointer))

(defcfun "rd_kafka_DescribeConfigs" :void
  (rk :pointer)
  (configs :pointer)
  (config-cnt size-t)
  (options :pointer)
  (rkqu :pointer))

(defcfun "rd_kafka_DescribeConfigs_result_resources" :pointer
  (result :pointer)
  (cntp :pointer))
