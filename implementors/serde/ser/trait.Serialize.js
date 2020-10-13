(function() {var implementors = {};
implementors["linked_hash_map"] = [{"text":"impl&lt;K, V, S&gt; Serialize for LinkedHashMap&lt;K, V, S&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;K: Serialize + Eq + Hash,<br>&nbsp;&nbsp;&nbsp;&nbsp;V: Serialize,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: BuildHasher,&nbsp;</span>","synthetic":false,"types":[]}];
implementors["serde"] = [];
implementors["serde_hjson"] = [{"text":"impl Serialize for Value","synthetic":false,"types":[]}];
implementors["serde_json"] = [{"text":"impl Serialize for Map&lt;String, Value&gt;","synthetic":false,"types":[]},{"text":"impl Serialize for Value","synthetic":false,"types":[]},{"text":"impl Serialize for Number","synthetic":false,"types":[]}];
implementors["toml"] = [{"text":"impl Serialize for Map&lt;String, Value&gt;","synthetic":false,"types":[]},{"text":"impl Serialize for Value","synthetic":false,"types":[]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()