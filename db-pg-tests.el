;;;  db-pg-tests - tests for postgresql database

(require 'db)
(require 'db-pg)
(require 'ert)

(ert-deftest db-pg ()
  "Test the reference creation stuff."
  (should
   (equal
    (list :db "my-db" :username "nic"
          :host "localhost" :password "" :port 5432
          :table "t1" :column "c1" :key "a")
    (plist-get
     ;; The reference has to start with db-pg symbol
     (db-pg (list 'db-pg
                  :db "my-db" :username "nic"
                  :table "t1" :column "c1" :key "a"))
     :pg-spec))))

(ert-deftest db-pg-make ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (should
     (equal
      (list :get 'db-pg/get :put 'db-pg/put :map 'db-pg/map
            :pg-spec (list :db "my-db" :username "nic"
                          :host "localhost" :password "" :port 5432
                          :table "t1" :column "c1" :key "a"))
      (db-make '(db-pg
                 :db "my-db" :username "nic"
                 :table "t1" :column "c1" :key "a"))))))

(ert-deftest db-pg/ref->spec ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (let ((db (db-make '(db-pg
                         :db "my-db" :username "nic"
                         :table "t1" :column "c1" :key "a"))))
      (should
       (equal
        (list "my-db" "nic" "" "localhost" 5432)
        (db-pg/ref->spec db))))))

;; This requires a working db
(ert-deftest db-pg/get ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (let ((db (db-make
               '(db-pg
                 :db "nictest1" :username "nferrier"
                 :table "t1" :column "c1" :key "a"))))
      (should
       (equal
        '((("g" . "10")("a" . "t1")))
        (db-get "t1" db))))))

(ert-deftest db-pg/put ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (let ((db (db-make
               '(db-pg
                 :db "nictest1" :username "nferrier"
                 :table "t1" :column "c1" :key "a"))))
      (should
       (equal
        '((("g" . "10")("a" . "t1")))
        (db-put "a" '(("a" . "t1")("g" . 10)) db))))))

(ert-deftest db-pg/map ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (let ((db (db-make
               '(db-pg
                 :db "nictest1" :username "nferrier"
                 :table "t1" :column "c1" :key "a"))))
      (should
       (equal
        '((("g" . "10")("a" . "t1")))
        (db-map (lambda (key val) val) db))))))

(ert-deftest db-pg/query ()
  (let ((db/types (db/make-type-store)))
    (puthash 'db-pg 'db-pg db/types)
    (let ((db (db-make
               '(db-pg
                 :db "nictest1" :username "nferrier"
                 :table "t1" :column "c1" :key "a"))))
      (should
       (equal
        '(("t1" ("g" . "10")("a" . "t1")))
        (db-query db '((= "g" 10))))))))

;;; db-pg-tests.el ends here
