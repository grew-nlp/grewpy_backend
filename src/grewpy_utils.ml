open Printf

open Conllx
open Libgrew

module Int_map = Map.Make (struct type t=int let compare=Stdlib.compare end)


exception Error of string

(* ==================================================================================================== *)
module Utils = struct
  let dot_to_png dot =
    let temp_file_name,out_ch = Filename.open_temp_file ~mode:[Open_rdonly;Open_wronly;Open_text] "grew_" ".dot" in
    fprintf out_ch "%s" dot;
    close_out out_ch;
    let png_file_name = Str.global_replace (Str.regexp ".dot") ".png" temp_file_name in
    ignore (Sys.command(sprintf "dot -Tpng -o %s %s " png_file_name temp_file_name));
    png_file_name

  let dep_to_png dep =
    let temp_file_name,out_ch = Filename.open_temp_file ~mode:[Open_rdonly;Open_wronly;Open_text] "grew_" ".dep" in
    fprintf out_ch "%s" dep;
    close_out out_ch;
    let png_file_name = Str.global_replace (Str.regexp ".dep") ".png" temp_file_name in
    ignore (Sys.command(sprintf "dep2pict %s %s " temp_file_name png_file_name));
    png_file_name

  let dep_to_svg dep =
    let temp_file_name,out_ch = Filename.open_temp_file ~mode:[Open_rdonly;Open_wronly;Open_text] "grew_" ".dep" in
    fprintf out_ch "%s" dep;
    close_out out_ch;
    let svg_file_name = Str.global_replace (Str.regexp ".dep") ".svg" temp_file_name in
    ignore (Sys.command(sprintf "dep2pict %s %s " temp_file_name svg_file_name));
    svg_file_name
end

(* ==================================================================================================== *)
module Global = struct

  let config = ref (Conllx_config.build "ud")

  let debug = ref false

  let (caller_pid: string option ref) = ref None

  let port = ref 8888

  (* the [grs_map] stores grs loaded by Python *)
  let (grs_map: Grs.t Int_map.t ref)  = ref Int_map.empty
  let grs_max = ref 0
  let grs_add grs =
    incr grs_max;
    grs_map := Int_map.add !grs_max grs !grs_map;
    !grs_max
  let grs_get index =
    try Int_map.find index !grs_map
    with Not_found -> raise (Error "Reference to an undefined grs")


  (* the [corpora_map] stores corpora loaded by Python *)
  let (corpora_map: Corpus.t Int_map.t ref) = ref Int_map.empty
  let corpus_max = ref 0

  let corpus_add corpus =
    incr corpus_max;
    corpora_map := Int_map.add !corpus_max corpus !corpora_map;
    !corpus_max

  let corpus_get index =
    try Int_map.find index !corpora_map 
    with Not_found -> raise (Error "Reference to an undefined corpus")
end

(* ==================================================================================================== *)
module Debug = struct
  let get_time () =
    let time = Unix.localtime (Unix.time ()) in
    sprintf "%02d/%02d/%04d %02d:%02d:%02d" time.Unix.tm_mday (time.Unix.tm_mon+1) (time.Unix.tm_year+1900) time.Unix.tm_hour time.Unix.tm_min time.Unix.tm_sec

  let log_ msg =
    if !Global.debug
    then printf "[grewpy - %s] %s\n%!" (get_time ()) msg

  let log msg = ksprintf log_ msg
end

(* ==================================================================================================== *)
module Sock = struct

  let start port =
    (* create the socket *)
    let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    let _ = Debug.log "Socket created" in

    (* bind the socket to the host machine on the given port *)
    let () = Unix.bind socket (Unix.ADDR_INET (Unix.inet_addr_loopback, port)) in
    let _ = Debug.log "Socket binded on port %d" port in

    (* let the socket listen *)
    let () = Unix.listen socket max_int in
    let _ = Debug.log "start listening on the socket" in

    socket

  let recv sock =
    let len_string = Bytes.create 10 in
    let _ = Unix.recv sock len_string 0 10 [] in
    let len = int_of_string (Bytes.to_string len_string) in

    let str = Bytes.create len in

    let rec loop already_read =
      if already_read < len
      then
        let recvlen = Unix.recv sock str already_read (len-already_read) [] in
        loop (already_read + recvlen) in
    loop 0;
    str

  let packet_size = 32768

  let send sock (str : string) =
    let bytes = Bytes.of_string str in
    let len = Bytes.length bytes in
    let _ = Unix.send sock (Bytes.of_string (Printf.sprintf "%010d" len)) 0 10 [] in

    let packet_nb = len / packet_size in
    for i = 0 to packet_nb -1  do
      let _ = Unix.send sock bytes (i*packet_size) packet_size [] in
      ()
    done;
    let _ = Unix.send sock bytes (packet_nb*packet_size) ((Bytes.length bytes) - (packet_nb*packet_size)) [] in
    Unix.close sock;
    ()

end (* module Sock *)

(* ==================================================================================================== *)
module Process = struct
  (** [error cmd] returns true iff the bash command [cmd] terminates without error *)
  let ok cmd =
    match Unix.close_process (Unix.open_process cmd) with
    | Unix.WEXITED 0 -> true
    | _ -> false
end (* module Process *)

(* ==================================================================================================== *)
module Periodic = struct
  (** execute periodically the function [fct ()] in a new thread.
      [delay] is the number of seconds between two calls  *)
  let rec start delay fct =
    ignore (
      Thread.create
        (fun () ->
           Unix.sleep delay; (* wait for the delay*)
           fct ();           (* run the function *)
           start delay fct   (* start a new thread for next execution *)
        ) ()
    )
end (* module Periodic *)

