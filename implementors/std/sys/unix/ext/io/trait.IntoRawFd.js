(function() {var implementors = {};
implementors["inotify"] = [{"text":"impl IntoRawFd for Inotify","synthetic":false,"types":[]}];
implementors["mio"] = [{"text":"impl IntoRawFd for TcpStream","synthetic":false,"types":[]},{"text":"impl IntoRawFd for TcpListener","synthetic":false,"types":[]},{"text":"impl IntoRawFd for UdpSocket","synthetic":false,"types":[]}];
implementors["same_file"] = [{"text":"impl IntoRawFd for Handle","synthetic":false,"types":[]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()