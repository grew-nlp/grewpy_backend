(lang dune 3.5)

(name grewpy_backend)
(version 0.2.0)

(generate_opam_files true)

(source
 (github grew-nlp/grewpy_backend))

(authors "Guillaume Bonfante and Bruno Guillaume")

(maintainers "Bruno Guillaume")

(license CeCILL)

(documentation https://grew.fr/python)

(package
 (name grewpy_backend)
 (synopsis "The Ocaml backend for the `grewpy` Python lib")
 (description "The Ocaml backend for the `grewpy` Python lib (https://pypi.org/project/grewpy)")
 (depends
  ocaml 
  dune 
  ANSITerminal 
  (amr (>= 0.6.0))
  containers 
  yojson 
  conll
  libcaml-dep2pict
  (libcaml-grew (>= 1.10.2))
  )
 (tags
  ("Grew" "Graph rewriting" "Python binding")))
