;;; ob-pyspark-sql.el --- Babel Functions for Pyspark Sql -*- lexical-binding: t; -*-

;; Copyright (C) 2024 Xuqing Jia

;; Author: Xuqing Jia <jxq@jxq.me>
;; URL: https://github.com/jxq0/ob-pyspark-sql
;; Version: 0.1
;; Package-Requires: ((emacs "27.1") (dash "2.19.1") (dash "1.10.0") (org "9.0.1"))
;; Keywords: convenience, org

;;; License:

;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:
;; Org-Babel support for evaluating spark sql with python.

;;; Code:
(require 'org-macs)
(require 'org-table)
(org-assert-version)

(require 'dash)
(require 's)
(require 'ob)
(require 'ob-python)

(defcustom ob-pyspark-sql-init-file nil
  "The python code to run."
  :type 'string
  :group 'ob-pyspark-sql)

(defvar ob-pyspark-sql-init-file-evaluated nil
  "Whether ob-pyspark-sql-init-file has been evalutated.")

(defcustom ob-pyspark-sql-udf-file nil
  "The python code to run."
  :type 'string
  :group 'ob-pyspark-sql)

(defvar ob-pyspark-sql-udf-file-evaluated nil
  "Whether ob-pyspark-sql-udf-file has been evalutated.")

(defcustom ob-pyspark-sql-main-file nil
  "The python code to run."
  :type 'string
  :group 'ob-pyspark-sql)

(defcustom ob-pyspark-sql-default-session "pyspark-sql"
  "The default python session."
  :type 'string
  :group 'ob-pyspark-sql)

(defcustom ob-pyspark-sql-enum-files nil
  "The default python session."
  :type 'string
  :group 'ob-pyspark-sql)

(defun ob-pyspark-sql-get-python-file (file-type)
  (let* ((default-file-var
          (pcase file-type
            ('main (list "main.py" ob-pyspark-sql-main-file t))
            ('init (list "init.py" ob-pyspark-sql-init-file
                         (not ob-pyspark-sql-init-file-evaluated)))
            ('udf (list "udf.py" ob-pyspark-sql-udf-file
                        (not ob-pyspark-sql-udf-file-evaluated)))
            (_ (error "invalid file type %s" file-type))))
         (file-name (car default-file-var))
         (custom-var (nth 1 default-file-var))
         (need-eval (nth 2 default-file-var))
         (real-file-name (if (and custom-var (file-exists-p custom-var))
                             custom-var
                           (concat (file-name-directory
                                    (symbol-file 'org-babel-execute:pyspark-sql))
                                   file-name))))
    (if need-eval
        (with-temp-buffer
          (insert-file-contents real-file-name)
          (buffer-string))
      "")))

(defun ob-pyspark-sql-input-tbl (input-tables-str)
  (if input-tables-str
      (->> input-tables-str
           (s-split ",")
           (mapcar
            (lambda (input-table)
              (when-let* ((tbl (org-babel-ref-resolve input-table))
                          (temp-file (make-temp-file "ob-pyspark-sql" nil ".csv")))
                (with-temp-file temp-file (insert (orgtbl-to-csv tbl nil)))
                (format "%s:%s" temp-file input-table))))
           (s-join ","))))

(defun org-babel-execute:pyspark-sql (body params)
  (-let* (((&alist :input-files :input-tables
                   :session :output-file :output-table
                   :enum)
           params)
          (input-tables-files (ob-pyspark-sql-input-tbl input-tables))
          (real-input-files
           (s-join "," (delq nil
                             (list input-files input-tables-files))))
          (real-output-table (or output-table ""))
          (real-output-file (or output-file ""))
          (real-session (if (string= session "none")
                            ob-pyspark-sql-default-session
                          session))
          (new-params (append
                       (list (cons :var (cons 'sql body))
                             (cons :var (cons 'input_files real-input-files))
                             (cons :var (cons 'output_table real-output-table))
                             (cons :var (cons 'output_file real-output-file))
                             (cons :var (cons 'enum_val enum))
                             (cons :var
                                   (cons 'enum_files_json
                                         ob-pyspark-sql-enum-files))
                             (cons :session real-session))
                       params))

          (python-code (format "%s\n%s\n%s"
                               (ob-pyspark-sql-get-python-file 'init)
                               (ob-pyspark-sql-get-python-file 'udf)
                               (ob-pyspark-sql-get-python-file 'main))))
    (org-babel-execute:python python-code new-params)))

(define-derived-mode pyspark-sql-mode
  sql-mode "pyspark-sql"
  "Major mode for pyspark sql.")

(provide 'ob-pyspark-sql)

;;; ob-pyspark-sql.el ends here
