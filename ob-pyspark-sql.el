(require 'org-macs)
(org-assert-version)

(require 'dash)
(require 's)
(require 'ob)
(require 'ob-python)

(defcustom ob-pyspark-sql-main-file nil
  "The python code to run."
  :type 'string
  :group 'ob-pyspark-sql)

(defcustom ob-pyspark-sql-default-session "pyspark-sql"
  "The default python session."
  :type 'string
  :group 'ob-pyspark-sql)

(defun ob-pyspark-sql-input-tbl (input-tables-str)
  (mapcar (lambda (input-table)
            (when-let* ((tbl (org-babel-ref-resolve input-table))
                        (temp-file (make-temp-file "ob-pyspark-sql")))
              (with-temp-file temp-file (insert (orgtbl-to-csv tbl nil)))
              (format "%s:%s" temp-file input-table)))
          (s-split "," input-tables-str)))

(defun org-babel-execute:pyspark-sql (body params)
  (-let* (((&alist :input_files :input_tables
                   :session :output_file :output_table)
           params)
          (input-tables-files (ob-pyspark-sql-input-tbl input_tables))
          (real-input-files (or input_files ""))
          (real-output-table (or output_table ""))
          (real-session (if (string= session "none")
                            ob-pyspark-sql-default-session
                          session))
          (new-params (append
                       (list (cons :var (cons 'sql body))
                             (cons :var (cons 'input_files real-input-files))
                             (cons :var (cons 'output_table real-output-table))
                             (cons :session real-session))
                       params))
          (main-file (if (and ob-pyspark-sql-main-file
                              (file-exists-p ob-pyspark-sql-main-file))
                         ob-pyspark-sql-main-file
                       (concat (file-name-directory
                                (symbol-file 'org-babel-execute:pyspark-sql))
                               "main.py")))
          (main (with-temp-buffer
                  (insert-file-contents main-file)
                  (buffer-string))))
    (org-babel-execute:python main new-params)))

(define-derived-mode pyspark-sql-mode
  sql-mode "pyspark-sql"
  "Major mode for pyspark sql.")

(provide 'ob-pyspark-sql)
