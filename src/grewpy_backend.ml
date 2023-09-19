open Printf
open Yojson.Basic.Util

open Conll
open Dep2pictlib
open Grewlib

open Grewpy_utils

(* ==================================================================================================== *)
module Args = struct
  let parse () =
    let rec loop = function
      | [] -> ()
      | "-d" :: tail | "--debug" :: tail -> Global.debug := true; loop tail
      | "-p" :: _ | "--port" :: _ -> failwith "--port option is removed, please upgrade grewpy"
      | "-c" :: c :: tail | "--caller" :: c :: tail-> Global.caller_pid := Some c; loop tail
      | x :: _ -> failwith (sprintf "[Args.parse] don't know what to do with arg: " ^ x)
    in loop (List.tl (Array.to_list Sys.argv))
end

(* ==================================================================================================== *)

let run_command_exc request =
  let _ = Debug.log "run request >>>>%s<<<<" request in
  let config = !Global.config in
  
  let json = 
    try Yojson.Basic.from_string request
    with _ -> json_error (sprintf "Cannot parse request: %s" request) in
  let command = 
    try 
      match json |> member "command" |> to_string_option with
      | Some c -> c
      | None -> failwith "no 'command' in request"
    with _ -> json_error (sprintf "Cannot parse command in request: %s" request) in


  try
    match command with
    (* ======================= set_config ======================= *)
    | "set_config" ->
      let config = 
        json
        |> member "config"
        |> to_string in
      Global.config := Conll_config.build config;
      ok `Null

    (* ======================= load_grs ======================= *)
    | "load_grs" ->
      let grs = match (member "file" json, member "str" json, member "json" json) with
      | (`String file, `Null, `Null) -> Grs.load ~config file
      | (`Null, `String str, `Null) -> Grs.parse ~config str
      | (`Null, `Null, json_gsr) -> Grs.of_json ~config json_gsr
      | _ -> json_error "wrong args in load_grs" in
        grs
        |> Global.grs_add
        |> (fun index -> `Assoc [("index", `Int index)])
        |> ok

    (* ======================= corpus_load ======================= *)
    | "corpus_load" ->
      let directory_opt = json |> member "directory" |> to_string_option in
      let files = json |> member "files" |> to_list |> filter_string in

      let complete_files = match directory_opt with
        | None -> files
        | Some dir -> List.map (fun file -> Filename.concat dir file) files in

      let conll_corpus = Conll_corpus.load_list ~quiet:true ~config complete_files in
      let corpus = Corpus.of_conllx_corpus conll_corpus in

      let index = Global.corpus_add corpus in
      let data = `Assoc [("index", `Int index); ("length", `Int (Corpus.size corpus)) ] in
      ok data

    (* ======================= corpus_clean ======================= *)
    | "corpus_clean" ->
      let index = json |> member "corpus_index" |> to_int in
      let _ = Global.corpus_clean index in
      ok `Null

    (* ======================= corpus_from_dict ======================= *)
    | "corpus_from_dict" ->
      begin
        let corpus = 
          json 
          |> member "graphs" 
          |> to_assoc
          |> List.map (fun (sent_id, json_graph) -> (sent_id, Graph.of_json json_graph))
          |> Corpus.from_assoc_list in
        let index = Global.corpus_add corpus in
        let data = `Assoc [("index", `Int index); ("length", `Int (Corpus.size corpus)) ] in
        ok data
      end

    (* ======================= corpus_update ======================= *)
    | "corpus_update" ->
      let corpus = json |> member "corpus_index" |> to_int |> Global.corpus_get in
      let graphs = json |> member "graphs" |> to_assoc in
      List.iter 
        (fun (sent_id, json_graph) ->
          Corpus.update_graph sent_id (Graph.of_json json_graph) corpus
        ) graphs;
      ok `Null

    (* ======================= corpus_get ======================= *)
    | "corpus_get" ->
      begin
        let corpus = json |> member "corpus_index" |> to_int |> Global.corpus_get in
        let graph = 
          match (json |> member "sent_id" |> to_string_option, json |> member "position" |> to_int_option) with
          | (Some sent_id, _) -> 
            begin
              match Corpus.graph_of_sent_id sent_id corpus with
              | Some g -> g
              | None -> json_error ("No graph with sent_id: " ^ sent_id)
            end 
          | (_, Some pos) -> Corpus.get_graph pos corpus
          | (None, None) -> json_error "neither sent_id or pos in the `corpus_get` request" in
        let data = Graph.to_json graph in
        ok data
      end

    (* ======================= corpus_get_all ======================= *)
    | "corpus_get_all" ->
      let corpus = json |> member "corpus_index" |> to_int |> Global.corpus_get in
      let data = `Assoc (Corpus.fold_right (fun sent_id graph acc -> (sent_id, Graph.to_json graph) :: acc) corpus []) in
      ok data

    (* ======================= corpus_size ======================= *)
    | "corpus_length" ->
      let corpus = json |> member "corpus_index" |> to_int |> Global.corpus_get in
      `Assoc [("status", `String "OK"); ("data", `Int (Corpus.size corpus))]

    (* ======================= corpus_sent_ids ======================= *)
    | "corpus_sent_ids" ->
      let corpus = json |> member "corpus_index" |> to_int |> Global.corpus_get in
      let sent_ids = Corpus.fold_right (fun sent_id _ acc -> (`String sent_id) :: acc) corpus [] in
      `Assoc [("status", `String "OK"); ("data", `List (sent_ids))]

    (* ======================= corpus_search ======================= *)
    | "corpus_search" ->
      let corpus_index = json |> member "corpus_index" |> to_int in
      let build_deco = json |> member "build_deco" |> to_bool in
      let request = 
        match json |> member "request" with
        | `Assoc ["index", `Int index] -> Global.request_get index 
        | x -> Request.of_json ~config x in
      let clustering_keys = 
        json 
        |> member "clustering_keys"
        |> to_list
        |> List.map (fun x -> Request.parse_cluster_item ~config request (to_string x)) in

      let corpus = Global.corpus_get corpus_index in
      let clustered_solutions =
        Corpus.search ~json_label:true
          ~config 
          [] 
          (fun sent_id graph matching acc ->
            let deco_json = 
              if build_deco
              then 
                let deco = Matching.build_deco request matching in
                [("deco", `Int (Global.deco_add deco))]
             else [] in
            `Assoc ([
              ("sent_id", `String sent_id);
              ("matching", Matching.to_json request graph matching)
            ]@deco_json) :: acc
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
      ok json



    (* ======================= corpus_count ======================= *)
    | "corpus_count" ->
      let corpus_index = json |> member "corpus_index" |> to_int in
      let request = 
        match json |> member "request" with
        | `Assoc ["index", `Int index] -> Global.request_get index 
        | x -> Request.of_json ~config x in
      let clustering_keys = 
        json 
        |> member "clustering_keys" 
        |> to_list
        |> List.map (fun x -> Request.parse_cluster_item ~config request (to_string x)) in
      let corpus = Global.corpus_get corpus_index in
      let clustered_count = Corpus.search ~config 0 (fun _ _ _ acc -> acc + 1) request clustering_keys corpus in
      let (json : Yojson.Basic.t) = Clustered.fold_layer
        (fun x -> `Int x)
        []
        (fun string_opt sub acc -> (CCOption.get_or ~default:"__undefined__" string_opt, sub) :: acc)
        (fun x -> `Assoc x)
        clustered_count in
      ok json

      (* ======================= corpus_count ======================= *)
    | "corpus_to_conll" ->
      let corpus_index = json |> member "corpus_index" |> to_int in
      let corpus = Global.corpus_get corpus_index in
      let buff = Buffer.create 32 in
      Corpus.iteri
        (fun _ _ graph -> 
          bprintf buff "%s\n" (graph |> Graph.to_json |> Conll.of_json |> Conll.to_string ~config)
        ) corpus;
      let conll = Buffer.contents buff in
      let json = `String conll in
      ok json

    (* ======================= grs_run_graph ======================= *)
    | "grs_run_graph" ->
      begin
        match (
          json |> member "graph" |> to_string_option,
          json |> member "grs_index" |> to_int_option,
          json |> member "strat" |> to_string_option
        ) with
        | Some graph, Some grs_index, Some strat ->
          let gr = Graph.of_json (Yojson.Basic.from_string graph) in
          let grs = Global.grs_get grs_index in
          let graph_list = Rewrite.simple_rewrite ~config gr grs strat in
          `Assoc [
                ("status", `String "OK");
                ("data", `List (List.map Graph.to_json graph_list))
              ]
        | _ -> json_error "incomplete 'grs_run_graph' command"
      end

    (* ======================= grs_run_corpus ======================= *)
    | "grs_run_corpus" ->
      begin
        match (
          json |> member "corpus_index" |> to_int_option,
          json |> member "grs_index" |> to_int_option,
          json |> member "strat" |> to_string_option
        ) with
        | Some corpus_index, Some grs_index, Some strat ->
          let corpus = Global.corpus_get corpus_index in

          let grs = Global.grs_get grs_index in
          let output = Corpus.fold_right
            (fun sent_id graph acc -> 
              let graph_list = Rewrite.simple_rewrite ~config graph grs strat in
              (sent_id,`List (List.map Graph.to_json graph_list)) :: acc
            ) corpus [] in
            `Assoc [
                ("status", `String "OK");
                ("data", `Assoc output)
              ]
        | _ -> json_error "incomplete 'grs_run_corpus' command"
      end

    (* ======================= grs_apply_graph ======================= *)
    | "grs_apply_graph" ->
      begin
        match (
          json |> member "graph" |> to_string_option,
          json |> member "grs_index" |> to_int_option,
          json |> member "strat" |> to_string_option
        ) with
        | Some graph, Some grs_index, Some strat ->
          let gr = Graph.of_json (Yojson.Basic.from_string graph) in
          let grs = Global.grs_get grs_index in
          let graph_list = Rewrite.simple_rewrite ~config gr grs strat in
          begin
             match graph_list with 
          | [one] ->
            `Assoc [
                ("status", `String "OK");
                ("data", Graph.to_json one)
              ]
          | _ -> json_error "not one output"
              end
        | _ -> json_error "incomplete 'grs_apply_graph' command"
      end
    (* ======================= grs_apply_corpus ======================= *)
    | "grs_apply_corpus" ->
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
          let grs = Global.grs_get grs_index in

          Corpus.iteri 
            (fun _ sent_id graph ->
              match Rewrite.simple_rewrite ~config graph grs strat with
              | [one] -> Corpus.update_graph sent_id one corpus
              | _ -> raise (Not_one sent_id)
            ) corpus;
            `Assoc [
                ("status", `String "OK");
                ("data", `Null)
              ]
        | _ -> json_error "incomplete 'grs_apply_corpus' command"
        with Not_one sent_id -> json_error (sprintf "Not one solution with sent_id = `%s`" sent_id)
      end

    (* ======================= json_grs ======================= *)
    | "json_grs" ->
      json
      |> member "grs_index"
      |> to_int
      |> Global.grs_get
      |> Grs.to_json ~config
      |> ok

    (* ======================= graph_to_svg ======================= *)
    | "graph_to_svg" ->
      let graph_to_dep g = match json |> member "deco" with
        | `Int deco_id -> 
          let deco = Global.deco_get deco_id in
          Graph.to_dep ~deco ~config g
        | _ -> Graph.to_dep ~config g in
      json
      |> member "graph"
      |> Graph.of_json
      |> graph_to_dep
      |> Dep2pictlib.from_dep
      |> Dep2pictlib.to_svg
      |> (fun s -> `String s)
      |> ok

    (* ======================= graph_to_dot ======================= *)
    | "graph_to_dot" ->
      json
      |> member "graph"
      |> Graph.of_json
      |> Graph.to_dot ~config
      |> (fun s -> `String s)
      |> ok

    (* ======================= graph_to_conll ======================= *)
    | "graph_to_conll" ->
      json
      |> member "graph"
      |> Conll.of_json
      |> Conll.to_string ~config
      |> (fun s -> `String s)
      |> ok

    (* ======================= graph_load ======================= *)
    | "graph_load" ->
      json
      |> member "file"
      |> to_string
      |> Graph.load ~config
      |> Graph.to_json
      |> ok

    (* ======================= request_parse ======================= *)
    | "request_parse" ->
      json
      |> member "request"
      |> to_string
      |> Request.parse ~config
      |> Global.request_add
      |> (fun index -> `Assoc [("index", `Int index)])
      |> ok
    | command -> json_error (sprintf "command '%s' not found" command)
  with
  | Json_error js -> raise (Json_error js)
  | Grewlib.Error msg -> json_error (sprintf "grewlib error: %s" msg)
  | Conll_error js -> raise (Json_error js)
  | exc -> json_error (sprintf " Unexpected error: %s"(Printexc.to_string exc))

let run_command request =
  try run_command_exc request
  with Json_error json -> `Assoc [("status", `String "ERROR"); ("message", json)]

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
    try Sock.start () with
    | Unix.Unix_error (Unix.EADDRINUSE,_,_) ->
      (* normal terminaison for automatic search of available port *)
      eprintf "[Grewpy] Port already used, failed to open socket\n"; exit 1
    | Unix.Unix_error (error,_,_) ->
      eprintf "[Grewpy] Unix error: %s\n" (Unix.error_message error); exit 1
    | exc ->
      eprintf "[Grewpy] Unexpected error: %s\n" (Printexc.to_string exc); exit 1
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
    let reply = Yojson.Basic.to_string (run_command (Bytes.to_string request)) in
    let _ = Debug.log "sending reply ==>%s<==" reply in
    Sock.send local_socket reply;
    Mutex.unlock m
  done
