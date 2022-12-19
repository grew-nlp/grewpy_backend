open Printf
open Yojson.Basic.Util

open Conllx
open Grew_types
open Libgrew

open Grewpy_utils

(* ==================================================================================================== *)
module Args = struct
  let parse () =
    let rec loop = function
      | [] -> ()
      | "-d" :: tail | "--debug" :: tail -> Global.debug := true; loop tail
      | "-p" :: p :: tail | "--port" :: p:: tail -> Global.port := int_of_string p; loop tail
      | "-c" :: c :: tail | "--caller" :: c :: tail-> Global.caller_pid := Some c; loop tail
      | x :: _ -> failwith (sprintf "[Ars.parse] don't know what to do with arg: " ^ x)
    in loop (List.tl (Array.to_list Sys.argv))
end


let json_error msg =
  Yojson.Basic.to_string (`Assoc [("status", `String "ERROR"); ("message", `String msg)])

(* ==================================================================================================== *)
let run_command request =
  let config = !Global.config in
  let _ = Debug.log "run request >>>>%s<<<<" request in
  try
    let json = Yojson.Basic.from_string request in
    match json |> member "command" |> to_string_option with
    | None -> failwith "no 'command' in request"

    (* ======================= set_config ======================= *)
    | Some "set_config" ->
      begin
        match json |> member "config" |> to_string_option with
        | Some c -> 
          begin
            try
              Global.config := Conllx_config.build c;
              Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", `Null)])
            with Conllx_error js -> 
            Yojson.Basic.to_string (`Assoc [("status", `String "ERROR"); ("message", js)])
          end
        | None -> json_error "no 'config' in 'set_config' command"
      end

      (* ======================= load_graph ======================= *)
    (* | Some "load_graph" ->
      begin
        match json |> member "filename" |> to_string_option with
        | Some filename ->
          let graph = Graph.load ~config filename in
          let data = Graph.to_json graph in
          Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", data)])
        | None -> json_error "no 'filename' in 'load_graph' command"
      end *)

    (* ======================= save_graph ======================= *)
    (* | Some "save_graph" ->
      begin
        match (
          json |> member "graph" |> to_string_option,
          json |> member "filename" |> to_string_option
        ) with
        | Some graph, Some filename ->
          let gr = Graph.of_json (Yojson.Basic.from_string graph) in
          Yojson.Basic.to_file filename (Graph.to_json gr);
          Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", `Null)])
        | _ -> json_error "incomplete 'save_graph' command"
      end *)

    (* ======================= load_grs ======================= *)
    | Some "load_grs" ->
      begin
        let dict = json |> to_assoc in
        match List.assoc_opt "filename" dict with
        | Some `String filename -> 
          let grs = Grs.load ~config filename in
          let index = Global.grs_add grs in
          let data = `Assoc [("index", `Int index)] in
          Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", data)])
        | _ -> 
          match List.assoc_opt "str" dict with
          | Some `String str ->  
            let grs = Grs.parse ~config str in
            let index = Global.grs_add grs in
            let data = `Assoc [("index", `Int index)] in
            Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", data)])
          | _ -> 
            match List.assoc_opt "json" dict with
            | Some _ ->
              let grs = json |> member "json" |> Grs.of_json ~config in
              let index = Global.grs_add grs in
              let data = `Assoc [("index", `Int index)] in
              Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", data)])
          | _ -> json_error "fail in 'load_grs' command"
      end

    (* ======================= corpus_load ======================= *)
    | Some "corpus_load" ->
      begin
        let directory_opt = json |> member "directory" |> to_string_option in
        let files = json |> member "files" |> to_list |> filter_string in

        let complete_files = match directory_opt with
          | None -> files
          | Some dir -> List.map (fun file -> Filename.concat dir file) files in

        try
          let conll_corpus = Conllx_corpus.load_list ~quiet:true ~config complete_files in
          let corpus = Corpus.of_conllx_corpus conll_corpus in

          let index = Global.corpus_add corpus in
          let data = `Assoc [("index", `Int index); ("length", `Int (Corpus.size corpus)) ] in
          Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", data)])
        with Conllx_error js ->
          Yojson.Basic.to_string (`Assoc [("status", `String "ERROR"); ("message", js)])
      end

    (* ======================= corpus_from_dict ======================= *)
    | Some "corpus_from_dict" ->
      begin
        let corpus = 
          json 
          |> member "graphs" 
          |> to_assoc
          |> List.map (fun (sent_id, json_graph) -> (sent_id, Graph.of_json json_graph))
          |> Corpus.from_assoc_list in
        let index = Global.corpus_add corpus in
        let data = `Assoc [("index", `Int index); ("length", `Int (Corpus.size corpus)) ] in
        Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", data)])
      end

    (* ======================= corpus_update ======================= *)
    | Some "corpus_update" ->
      begin
        let corpus = json |> member "corpus_index" |> to_int |> Global.corpus_get in
        let graphs = json |> member "graphs" |> to_assoc in
        List.iter 
          (fun (sent_id, json_graph) ->
            Corpus.update_graph sent_id (Graph.of_json json_graph) corpus
          ) graphs;
        Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", `Null)])
      end

    (* ======================= corpus_get ======================= *)
    | Some "corpus_get" ->
      begin
        try
          let corpus = json |> member "corpus_index" |> to_int |> Global.corpus_get in
          let graph = 
            match (json |> member "sent_id" |> to_string_option, json |> member "position" |> to_int_option) with
            | (Some sent_id, _) -> 
              begin
                match Corpus.graph_of_sent_id sent_id corpus with
                | Some g -> g
                | None -> raise (Error ("No graph with sent_id: " ^ sent_id))
              end 
            | (_, Some pos) -> Corpus.get_graph pos corpus
            | (None, None) -> raise (Error "neither sent_id or pos in the request") in
          let data = Graph.to_json graph in
          Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", data)])
        with
        | Error msg -> json_error msg
      end

    (* ======================= corpus_get_all ======================= *)
    | Some "corpus_get_all" ->
      begin
        try
          let corpus = json |> member "corpus_index" |> to_int |> Global.corpus_get in
          let data = `Assoc (Corpus.fold_right (fun sent_id graph acc -> (sent_id, Graph.to_json graph) :: acc) corpus []) in
          Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", data)])
        with
        | Error msg -> json_error msg
      end

    (* ======================= corpus_size ======================= *)
    | Some "corpus_length" ->
      begin
        try
          let corpus = json |> member "corpus_index" |> to_int |> Global.corpus_get in
          Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", `Int (Corpus.size corpus))])
        with
        | Error msg -> json_error msg
      end

    (* ======================= corpus_sent_ids ======================= *)
    | Some "corpus_sent_ids" ->
      begin
        try
          let corpus = json |> member "corpus_index" |> to_int |> Global.corpus_get in
          let sent_ids = Corpus.fold_right (fun sent_id _ acc -> (`String sent_id) :: acc) corpus [] in

          Yojson.Basic.to_string (`Assoc [("status", `String "OK"); ("data", `List (sent_ids))])
        with
        | Error msg -> json_error msg
      end

    (* ======================= corpus_search ======================= *)
    | Some "corpus_search" ->
      begin
        try
          let corpus_index = json |> member "corpus_index" |> to_int in
          let request = Request.of_json ~config (json |> member "request") in
          let clustering_keys = 
            json 
            |> member "clustering_keys" 
            |> to_list 
            |> List.map (fun x -> Key (to_string x)) in

          let corpus = Global.corpus_get corpus_index in
          let clustered_solutions = 
            Corpus.search 
              ~config 
              [] 
              (fun sent_id graph matching acc -> `Assoc [
                ("sent_id", `String sent_id);
                ("matching", Matching.to_json request graph matching)
                ] :: acc
              )
              request 
              clustering_keys 
              corpus in
          let (json : Yojson.Basic.t) = Clustered.fold_layer
            (fun x -> `List x)
            []
            (fun string_opt sub acc -> (CCOption.get_or ~default:"__undefined__" string_opt, sub) :: acc)
            (fun x -> `Assoc x)
            clustered_solutions in
  
          Yojson.Basic.to_string
            (`Assoc [
                ("status", `String "OK");
                ("data", json)
              ])
        with
        | Error msg -> json_error msg
      end

    (* ======================= corpus_count ======================= *)
    | Some "corpus_count" ->
      begin
        try
          let corpus_index = json |> member "corpus_index" |> to_int in
          let request = Request.of_json ~config (json |> member "request") in
          let clustering_keys = 
            json 
            |> member "clustering_keys" 
            |> to_list 
            |> List.map (fun x -> Key (to_string x)) in

          let corpus = Global.corpus_get corpus_index in
          let clustered_count = Corpus.search ~config 0 (fun _ _ _ acc -> acc + 1) request clustering_keys corpus in
          let (json : Yojson.Basic.t) = Clustered.fold_layer
            (fun x -> `Int x)
            []
            (fun string_opt sub acc -> (CCOption.get_or ~default:"__undefined__" string_opt, sub) :: acc)
            (fun x -> `Assoc x)
            clustered_count in

          Yojson.Basic.to_string
            (`Assoc [("status", `String "OK"); ("data", json)])
        with
        | Error msg -> json_error msg
      end

    (* ======================= grs_run_graph ======================= *)
    | Some "grs_run_graph" ->
      begin
        match (
          json |> member "graph" |> to_string_option,
          json |> member "grs_index" |> to_int_option,
          json |> member "strat" |> to_string_option
        ) with
        | Some graph, Some grs_index, Some strat ->
          let gr = Graph.of_json (Yojson.Basic.from_string graph) in
          let grs = try Global.grs_get grs_index with Not_found -> raise (Error "Reference to an undefined GRS") in
          let graph_list = Rewrite.simple_rewrite ~config gr grs strat in
          Yojson.Basic.to_string
            (`Assoc [
                ("status", `String "OK");
                ("data", `List (List.map Graph.to_json graph_list))
              ])
        | _ -> json_error "incomplete 'grs_run_graph' command"
      end

    (* ======================= grs_run_corpus ======================= *)
    | Some "grs_run_corpus" ->
      begin
        match (
          json |> member "corpus_index" |> to_int_option,
          json |> member "grs_index" |> to_int_option,
          json |> member "strat" |> to_string_option
        ) with
        | Some corpus_index, Some grs_index, Some strat ->
          let corpus = Global.corpus_get corpus_index in

          let grs = try Global.grs_get grs_index with Not_found -> raise (Error "Reference to an undefined GRS") in
          let output = Corpus.fold_right
            (fun sent_id graph acc -> 
              let graph_list = Rewrite.simple_rewrite ~config graph grs strat in
              (sent_id,`List (List.map Graph.to_json graph_list)) :: acc
            ) corpus [] in
          Yojson.Basic.to_string
            (`Assoc [
                ("status", `String "OK");
                ("data", `Assoc output)
              ])
        | _ -> json_error "incomplete 'grs_run_corpus' command"
      end

    (* ======================= grs_apply_graph ======================= *)
    | Some "grs_apply_graph" ->
      begin
        match (
          json |> member "graph" |> to_string_option,
          json |> member "grs_index" |> to_int_option,
          json |> member "strat" |> to_string_option
        ) with
        | Some graph, Some grs_index, Some strat ->
          let gr = Graph.of_json (Yojson.Basic.from_string graph) in
          let grs = try Global.grs_get grs_index with Not_found -> raise (Error "Reference to an undefined GRS") in
          let graph_list = Rewrite.simple_rewrite ~config gr grs strat in
          begin
             match graph_list with 
          | [one] -> Yojson.Basic.to_string
            (`Assoc [
                ("status", `String "OK");
                ("data", Graph.to_json one)
              ])
          | _ -> json_error "not one output"
              end
        | _ -> json_error "incomplete 'grs_apply_graph' command"
      end
    (* ======================= grs_apply_corpus ======================= *)
    | Some "grs_apply_corpus" ->
      let exception Not_one of string in
      begin
        try
        match (
          json |> member "corpus_index" |> to_int_option,
          json |> member "grs_index" |> to_int_option,
          json |> member "strat" |> to_string_option
        ) with
        | Some corpus_index, Some grs_index, Some strat ->
          let corpus = Global.corpus_get corpus_index in
          let grs = try Global.grs_get grs_index with Not_found -> raise (Error "Reference to an undefined GRS") in

          Corpus.iteri 
            (fun _ sent_id graph ->
              match Rewrite.simple_rewrite ~config graph grs strat with
              | [one] -> Corpus.update_graph sent_id one corpus
              | _ -> raise (Not_one sent_id)
            ) corpus;
          Yojson.Basic.to_string
            (`Assoc [
                ("status", `String "OK");
                ("data", `Null)
              ])
        | _ -> json_error "incomplete 'grs_apply_corpus' command"
        with Not_one sent_id -> json_error (sprintf "Not one solution with sent_id = `%s`" sent_id)
      end

    (* ======================= json_grs ======================= *)
    | Some "json_grs" ->
      begin
        match json |> member "grs_index" |> to_int_option with
        | Some grs_index ->
          let grs = try Global.grs_get grs_index with Not_found -> raise (Error "Reference to an undefined GRS") in
          Yojson.Basic.to_string
            (`Assoc [
                ("status", `String "OK");
                ("data", Grs.to_json ~config grs)
              ])
        | _ -> json_error "incomplete 'json_grs' command"
      end

      (* ======================= conll_graph ======================= *)
    | Some "conll_graph" ->
      begin
        match json |> member "graph" |> to_string_option with
        | Some graph ->
          let conll_string =
          graph
          |> Yojson.Basic.from_string
          |> (fun x -> printf "==========================\n%s\n======================\n%!" (Yojson.Basic.pretty_to_string x); x)
          |> Conllx.of_json
          |> Conllx.to_string ~config in
          Yojson.Basic.to_string
            (`Assoc [
                ("status", `String "OK");
                ("data", `String conll_string)
              ])
        | _ -> json_error "cannot execute `conll_graph' command"
      end

    (* ======================= dot_graph ======================= *)
    (* | Some "dot_to_png" ->
      begin
        match json |> member "graph" |> to_string_option with
        | Some graph ->
          let gr = Graph.of_json (Yojson.Basic.from_string graph) in
          let dot = Graph.to_dot ~config gr in
          let png_file = Utils.dot_to_png dot in
          Yojson.Basic.to_string
            (`Assoc [
                ("status", `String "OK");
                ("data", `String png_file)
              ])
        | _ -> json_error "incomplete 'dot_graph' command"
      end *)

    (* ======================= dot_graph ======================= *)
    (* | Some "dep_to_png" ->
      begin
        match json |> member "graph" |> to_string_option with
        | Some graph ->
          let gr = Graph.of_json (Yojson.Basic.from_string graph) in
          let dep = Graph.to_dep ~config gr in
          let png_file = Utils.dep_to_png dep in
          Yojson.Basic.to_string
            (`Assoc [
                ("status", `String "OK");
                ("data", `String png_file)
              ])
        | _ -> json_error "incomplete 'dep_graph' command"
      end *)

    (* ======================= graph_svg ======================= *)
    (* | Some "dep_to_svg" ->
      begin
        match json |> member "graph" |> to_string_option with
        | Some graph ->
          let gr = Graph.of_json (Yojson.Basic.from_string graph) in
          let svg_file = Utils.dep_to_svg (Graph.to_dep ~config gr) in
          Yojson.Basic.to_string
            (`Assoc [
                ("status", `String "OK");
                ("data", `String svg_file)
              ])
        | _ -> json_error "incomplete 'graph_svg' command"
      end *)

    | Some command -> json_error (sprintf "command '%s' not found" command)
  with
  | Libgrew.Error msg -> json_error msg
  | exc -> json_error (Printexc.to_string exc)

(* ==================================================================================================== *)
(* Main *)
(* ==================================================================================================== *)
let _ =
  let _ = Args.parse () in

  (* if the caller is known, check if it's alive *)
  let _ = match !Global.caller_pid with
    | None -> ()
    | Some pid ->
      let stop_if_caller_is_dead () =
        if not (Process.ok (sprintf "kill -0 %s 2> /dev/null" pid))
        then (Debug.log "\ncaller (pid:%s) was stopped, I stop. Bye" pid; exit 0) in
      (* check periodically if the caller is still alive *)
      Periodic.start 10 stop_if_caller_is_dead in

  let socket =
    try Sock.start !Global.port with
    | Unix.Unix_error (Unix.EADDRINUSE,_,_) ->
      (* normal terminaison for automatic search of available port *)
      printf "[Grewpy] Port %d already used, failed to open socket\n" !Global.port; exit 0
    | Unix.Unix_error (error,_,_) ->
      printf "[Grewpy] Unix error: %s\n" (Unix.error_message error); exit 1
    | exc ->
      printf "[Grewpy] Unexpected error: %s\n" (Printexc.to_string exc); exit 1
  in

  let m = Mutex.create () in

  while true do
    let _ = Debug.log "ready to receive data" in
    let (local_socket, _) = Unix.accept socket in
    let _ = Debug.log "connection accepted, reading request" in
    let request = Sock.recv local_socket in
    let _ = Debug.log "request received" in
    Mutex.lock m;
    Debug.log "start to handle request ==>%s<==" (Bytes.to_string request);
    let reply = run_command (Bytes.to_string request) in
    let _ = Debug.log "sending reply ==>%s<==" reply in
    Sock.send local_socket reply;
    Mutex.unlock m
  done
