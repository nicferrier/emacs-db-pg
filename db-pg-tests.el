;;;  db-pg-tests - tests for postgresql database

(require 'db)
(require 'db-pg)
(require 'ert)
(require 'cl)

;; All of these tests have the problem that they need my database to
;; create them



(defconst db-pg-tests-db "emacs-db-pg-test"
  "The name we use for the tests.")

;; The creation of the database should:
;;
;; * create the db `db-pg-tests-db'
;; * turn on the hstore extension in it
;;    psql `db-pg-tests-db' -e 'CREATE EXTENSION hstore';


(ert-deftest db-pg ()
  "Test the reference creation stuff."
  (should
   (equal
    (list :db db-pg-tests-db :username "nferrier"
          :host "localhost" :password "" :port 5432
          :table "t1" :column "c1" :key "a")
    (plist-get
     ;; The reference has to start with db-pg symbol
     (db-pg (list 'db-pg
                  :db db-pg-tests-db :username "nferrier"
                  :table "t1" :column "c1" :key "a"))
     :pg-spec))))

(ert-deftest db-pg-make ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (should
     (equal
      (list :get 'db-pg/get :put 'db-pg/put :map 'db-pg/map
            :pg-spec (list :db db-pg-tests-db :username "nferrier"
                          :host "localhost" :password "" :port 5432
                          :table "t1" :column "c1" :key "a"))
      (db-make `(db-pg
                 :db ,db-pg-tests-db :username "nferrier"
                 :table "t1" :column "c1" :key "a"))))))

(ert-deftest db-pg/ref->spec ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (let ((db (db-make `(db-pg
                         :db ,db-pg-tests-db :username "nferrier"
                         :table "t1" :column "c1" :key "a"))))
      (should
       (equal
        (list db-pg-tests-db "nferrier" "" "localhost" 5432)
        (db-pg/ref->spec db))))))

(defun db-pg/truncate (db &optional con)
  "Truncate the table specified in DB.

If CON is supplied it's presumed to be an open DB connection."
  (flet ((truncate (con table)
           (pg:exec
            con
            (format "truncate table %s" table))))
    (let* ((pg-spec (plist-get db :pg-spec))
           (table (plist-get pg-spec :table)))
      (if con
          (truncate con table)
          (with-pg-connection con (db-pg/ref->spec db)
            (truncate con table))))))

(defun db-pg/test-insert (db alist)
  "Testing function inserts ALIST as an HSTORE into DB."
  (with-pg-connection con (db-pg/ref->spec db)
    (let* ((pg-spec (plist-get db :pg-spec))
           (table (plist-get pg-spec :table))
           (column (plist-get pg-spec :column))
           (vals (mapconcat
                  (lambda (v)
                    (format
                     "%s=>%s"
                     (car v) (cdr v))) alist ",")))
      (db-pg/truncate db con)
      (pg:exec
       con
       (format "insert into %s (%s) values ('%s')"
               table column vals)))))

;; This requires a working db
(ert-deftest db-pg/get ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (let ((db (db-make
               `(db-pg
                 :db ,db-pg-tests-db :username "nferrier"
                 :table "t1" :column "c1" :key "a"))))
      (db-pg/test-insert db '(("g" . 10)(a . t1)))
      (should
       (equal
        '(("g" . "10")("a" . "t1"))
        (db-get "t1" db))))))

(ert-deftest db-pg/put ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (let ((db (db-make
               `(db-pg
                 :db ,db-pg-tests-db :username "nferrier"
                 :table "t1" :column "c1" :key "a"))))
      ;; Test an insert
      (db-pg/truncate db)
      (should
       (equal
        '(("g" . "12")("a" . "t1"))
        (db-put "a" '(("a" . "t1")("g" . 12)) db)))
      ;; Test an update
      (should
       (equal
        '(("g" . "10")("a" . "t1"))
        (db-put "a" '(("a" . "t1")("g" . 10)) db))))))

(ert-deftest db-pg/map ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (let ((db (db-make
               `(db-pg
                 :db ,db-pg-tests-db :username "nferrier"
                 :table "t1" :column "c1" :key "a"))))
      (db-pg/test-insert db '(("g" . 10)(a . t1)))
      (should
       (equal
        '((("g" . "10")("a" . "t1")))
        (db-map (lambda (key val) val) db))))))

(ert-deftest db-pg/query ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (let ((db (db-make
               `(db-pg
                 :db ,db-pg-tests-db :username "nferrier"
                 :table "t1" :column "c1" :key "a"))))
      (should
       (equal
        '(("t1" ("g" . "10")("a" . "t1")))
        (db-query db '((= "g" 10))))))))

;;; db-pg-tests.el ends here
