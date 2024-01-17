(require 'org-macs)
(org-assert-version)

(require 'dash)
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

(defun org-babel-execute:pyspark-sql (body params)
  (-let* (((&alist :csv_files :csv_files_map :session) params)
          (real-session (if (string= session "none")
                            ob-pyspark-sql-default-session
                          session))
          (new-params (append
                       (list (cons :var (cons 'sql body))
                             (cons :var (cons 'csv_files csv_files))
                             (cons :var (cons 'csv_files_map csv_files_map))
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
    (message "%s" load-file-name)
    (org-babel-execute:python main new-params)))

(define-derived-mode pyspark-sql-mode
  sql-mode "pyspark-sql"
  "Major mode for pyspark sql.")

(provide 'ob-pyspark-sql)
