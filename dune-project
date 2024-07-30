(lang dune 3.5)

(name grewpy_backend)
(version 0.5.4)

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
  dune-build-info
  ANSITerminal 
  (amr (>= 0.6.0))
  containers 
  yojson 
  conll
  dep2pictlib
  (grewlib (>= 1.13.0))
  )
 (tags
  ("Grew" "Graph rewriting" "Python binding")))
