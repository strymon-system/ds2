(function() {var implementors = {};
implementors["ansi_term"] = [{"text":"impl&lt;'a, S:&nbsp;'a + ToOwned + ?Sized&gt; Deref for ANSIGenericString&lt;'a, S&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;&lt;S as ToOwned&gt;::Owned: Debug,&nbsp;</span>","synthetic":false,"types":[]}];
implementors["arrayvec"] = [{"text":"impl&lt;A&gt; Deref for ArrayString&lt;A&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;A: Array&lt;Item = u8&gt; + Copy,&nbsp;</span>","synthetic":false,"types":[]},{"text":"impl&lt;A:&nbsp;Array&gt; Deref for ArrayVec&lt;A&gt;","synthetic":false,"types":[]}];
implementors["iovec"] = [{"text":"impl Deref for IoVec","synthetic":false,"types":[]}];
implementors["mio"] = [{"text":"impl Deref for UnixReady","synthetic":false,"types":[]}];
implementors["petgraph"] = [{"text":"impl&lt;'a, G&gt; Deref for Frozen&lt;'a, G&gt;","synthetic":false,"types":[]},{"text":"impl&lt;'b, T&gt; Deref for Ptr&lt;'b, T&gt;","synthetic":false,"types":[]}];
implementors["regex_syntax"] = [{"text":"impl Deref for Literal","synthetic":false,"types":[]}];
implementors["serde"] = [{"text":"impl&lt;'a&gt; Deref for Bytes&lt;'a&gt;","synthetic":false,"types":[]},{"text":"impl Deref for ByteBuf","synthetic":false,"types":[]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()