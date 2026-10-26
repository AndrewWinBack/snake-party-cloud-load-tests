"""Add read-error logging to pinned official k6; do not change execution logic."""
from pathlib import Path
import subprocess

commit = 'e0887846143ab176d4b5483c9d52cf3b3e009f1a'  # official v2.3.0
root = Path('runtime-k6')
subprocess.run(['git', 'init', str(root)], check=True)
subprocess.run(['git', '-C', str(root), 'fetch', '--depth=1', 'https://github.com/grafana/k6.git', commit], check=True)
subprocess.run(['git', '-C', str(root), 'checkout', '--detach', 'FETCH_HEAD'], check=True)
assert subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip() == commit
source = root / 'internal/js/modules/k6/websockets/websockets.go'
text = source.read_text()
needle = '\t\tif !websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {'
assert text.count(needle) == 1
diagnostic = r'''
		// Preserve the original error before the existing k6 filter can hide it.
		// No state, metric, workload, close handling, or threshold changes.
		if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			player, _ := w.tagsAndMeta.Tags.Get("player")
			fmt.Printf("{\"event\":\"transport_read_error\",\"player\":%q,\"cause\":%q,\"errorType\":%q,\"at\":%d}\n", player, err.Error(), fmt.Sprintf("%T", err), time.Now().UnixMilli())
		}
'''
source.write_text(text.replace(needle, diagnostic + needle))
subprocess.run(['go', 'build', '-trimpath', '-o', str(Path.cwd() / 'k6'), '.'], cwd=root, check=True)
subprocess.run(['./k6', 'version'], check=True)
