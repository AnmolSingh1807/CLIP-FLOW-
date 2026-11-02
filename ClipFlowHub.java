import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ClipFlow - zero-dependency LAN clipboard + file sharing hub.
 *
 *   javac ClipFlowHub.java
 *   java ClipFlowHub [port]
 *
 * Pure JDK 11+: com.sun.net.httpserver, java.io, java.net, java.util(.concurrent).
 * No AWT, no third-party libraries. The web UI is embedded below as a text block.
 */
public class ClipFlowHub {

    // ------------------------------------------------------------------ config
    static final int DEFAULT_PORT = 8080;
    static final long PEER_TTL_MS = 8_000L;                    // evict after 8s without heartbeat
    static final long MAX_FILE_BYTES = 2L * 1024 * 1024 * 1024; // 2 GiB per file
    static final int MAX_TEXT_BYTES = 1024 * 1024;             // 1 MiB per text packet
    static final int MAX_PACKETS = 500;
    static final long PACKET_TTL_MS = 60L * 60 * 1000;         // feed items live 1 hour

    // ------------------------------------------------------------------- state
    static final ConcurrentHashMap<String, PeerDevice> peers = new ConcurrentHashMap<>();
    static final CopyOnWriteArrayList<TransferPacket> packets = new CopyOnWriteArrayList<>();
    static final AtomicLong seq = new AtomicLong(System.currentTimeMillis());
    static final Set<String> RESERVED = new HashSet<>(Arrays.asList(
            "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"));

    static File dropDir;
    static File dropDirCanon;
    static volatile String primaryUrl;
    static volatile List<String> allUrls = new ArrayList<>();
    static PrintStream out = System.out;

    static final class PeerDevice {
        final String id;
        volatile String alias = "Unknown";
        volatile String type = "desktop";
        volatile String ip = "";
        volatile long lastSeen;
        PeerDevice(String id) { this.id = id; }
    }

    static final class TransferPacket {
        final String id, fromId, fromAlias, toId, type, text, fileName;
        final long size, ts;
        TransferPacket(String id, String fromId, String fromAlias, String toId, String type,
                       String text, String fileName, long size, long ts) {
            this.id = id; this.fromId = fromId; this.fromAlias = fromAlias; this.toId = toId;
            this.type = type; this.text = text; this.fileName = fileName; this.size = size; this.ts = ts;
        }
        String toJson() {
            StringBuilder sb = new StringBuilder(128);
            sb.append("{\"id\":").append(q(id))
              .append(",\"fromId\":").append(q(fromId))
              .append(",\"from\":").append(q(fromAlias))
              .append(",\"toId\":").append(q(toId))
              .append(",\"mode\":").append(q("ALL".equals(toId) ? "broadcast" : "direct"))
              .append(",\"type\":").append(q(type))
              .append(",\"ts\":").append(ts);
            if ("text".equals(type)) sb.append(",\"text\":").append(q(text));
            else sb.append(",\"fileName\":").append(q(fileName)).append(",\"size\":").append(size);
            return sb.append('}').toString();
        }
    }

    interface Route { void handle(HttpExchange ex) throws Exception; }

    // -------------------------------------------------------------------- main
    public static void main(String[] args) throws Exception {
        out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, "UTF-8");

        int port = DEFAULT_PORT;
        boolean plainQr = false;
        String envPort = System.getenv("PORT");
        if (envPort != null) { try { port = Integer.parseInt(envPort.trim()); } catch (NumberFormatException ignored) { } }
        for (String a : args) {
            if ("--plain".equals(a)) plainQr = true;
            else { try { port = Integer.parseInt(a.trim()); } catch (NumberFormatException ignored) { } }
        }
        if (port < 1 || port > 65535) port = DEFAULT_PORT;

        dropDir = new File("shared_drops");
        if (!dropDir.isDirectory() && !dropDir.mkdirs()) {
            out.println("ERROR: cannot create folder " + dropDir.getAbsolutePath());
            System.exit(1);
        }
        dropDirCanon = dropDir.getCanonicalFile();

        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);   // 0.0.0.0 -> reachable on the LAN
        } catch (BindException e) {
            out.println("ERROR: port " + port + " is already in use. Close the other program or run: java ClipFlowHub <otherPort>");
            System.exit(1);
            return;
        }

        server.createContext("/", wrap(ClipFlowHub::handleRoot));
        server.createContext("/api/heartbeat", wrap(ClipFlowHub::handleHeartbeat));
        server.createContext("/api/peers", wrap(ClipFlowHub::handlePeers));
        server.createContext("/api/send", wrap(ClipFlowHub::handleSend));
        server.createContext("/api/inbox", wrap(ClipFlowHub::handleInbox));
        server.createContext("/api/download", wrap(ClipFlowHub::handleDownload));
        server.createContext("/api/info", wrap(ClipFlowHub::handleInfo));
        server.createContext("/api/qr", wrap(ClipFlowHub::handleQr));

        // Fixed pool: a long file upload occupies one thread, never the whole server.
        server.setExecutor(Executors.newFixedThreadPool(10));

        ScheduledExecutorService janitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "clipflow-janitor");
            t.setDaemon(true);
            return t;
        });
        janitor.scheduleAtFixedRate(ClipFlowHub::sweep, 1, 1, TimeUnit.SECONDS);

        List<String> ips = findLanAddresses();
        allUrls = new ArrayList<>();
        for (String ip : ips) allUrls.add("http://" + ip + ":" + port);
        boolean noLan = allUrls.isEmpty();
        if (noLan) allUrls.add("http://localhost:" + port);
        primaryUrl = allUrls.get(0);

        server.start();
        final HttpServer srv = server;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { srv.stop(0); }));

        printBanner(primaryUrl, noLan);
        try {
            QrCode qr = QrCode.encode(primaryUrl);
            out.println();
            printQr(qr, !plainQr && ansiSupported());
            out.println();
            out.println("  Scan the QR code with your phone camera (same Wi-Fi), or type the address.");
        } catch (RuntimeException e) {
            out.println("  (QR code unavailable: " + e.getMessage() + ")");
        }
        if (allUrls.size() > 1) {
            out.println();
            out.println("  Other addresses of this machine:");
            for (int i = 1; i < allUrls.size(); i++) out.println("    " + allUrls.get(i));
        }
        out.println();
        out.println("  Received files are saved in: " + dropDirCanon.getAbsolutePath());
        out.println("  Press Ctrl+C to stop.");
        out.println();
    }

    // ----------------------------------------------------------------- startup
    static void printBanner(String url, boolean noLan) {
        List<String> lines = new ArrayList<>();
        lines.add("ClipFlow  -  local clipboard & file sharing");
        lines.add("");
        lines.add(noLan ? "No LAN address found - connect to Wi-Fi / hotspot first." : "Open this on any device on the same Wi-Fi:");
        lines.add("");
        lines.add("    " + url);
        int w = 0;
        for (String l : lines) w = Math.max(w, l.length());
        w += 4;
        StringBuilder bar = new StringBuilder("  +");
        for (int i = 0; i < w; i++) bar.append('-');
        bar.append('+');
        out.println();
        out.println(bar);
        for (String l : lines) {
            StringBuilder sb = new StringBuilder("  |  ").append(l);
            while (sb.length() < w + 3) sb.append(' ');
            out.println(sb.append('|'));
        }
        out.println(bar);
    }

    static boolean ansiSupported() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (!os.contains("win")) return true;
        return System.getenv("WT_SESSION") != null || System.getenv("ANSICON") != null
                || System.getenv("TERM") != null || "ON".equals(System.getenv("ConEmuANSI"));
    }

    /** Half-block rendering: two QR rows per text line. */
    static void printQr(QrCode qr, boolean ansi) {
        int b = 3, dim = qr.size + 2 * b;
        for (int y = 0; y < dim; y += 2) {
            StringBuilder sb = new StringBuilder("  ");
            for (int x = 0; x < dim; x++) {
                boolean top = qr.isDark(x - b, y - b), bot = qr.isDark(x - b, y + 1 - b);
                if (ansi) {
                    sb.append("\u001b[").append(top ? 30 : 97).append(';').append(bot ? 40 : 107).append("m\u2580");
                } else {
                    // inverted: light modules are filled, so it reads correctly on dark terminals
                    sb.append(top ? (bot ? ' ' : '\u2584') : (bot ? '\u2580' : '\u2588'));
                }
            }
            if (ansi) sb.append("\u001b[0m");
            out.println(sb);
        }
    }

    /** Picks real LAN IPv4 addresses (Wi-Fi / Ethernet first, virtual adapters last). */
    static List<String> findLanAddresses() {
        final Map<String, Integer> scored = new LinkedHashMap<>();
        try {
            Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
            while (nis != null && nis.hasMoreElements()) {
                NetworkInterface ni = nis.nextElement();
                if (!ni.isUp() || ni.isLoopback()) continue;
                String name = (ni.getName() + " " + ni.getDisplayName()).toLowerCase();
                int score = 0;
                String[] virtual = {"docker", "veth", "vmnet", "vbox", "virtualbox", "vethernet", "hyper-v", "wsl",
                        "br-", "tun", "tap", "utun", "awdl", "llw", "bridge", "virbr", "vpn", "loopback"};
                for (String v : virtual) if (name.contains(v)) { score -= 50; break; }
                String[] good = {"wlan", "wlp", "wi-fi", "wifi", "wireless", "en0", "eth", "ethernet", "enp", "eno"};
                for (String g : good) if (name.contains(g)) { score += 5; break; }
                Enumeration<InetAddress> addrs = ni.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    InetAddress a = addrs.nextElement();
                    if (!(a instanceof Inet4Address) || a.isLoopbackAddress() || a.isLinkLocalAddress()) continue;
                    String ip = a.getHostAddress();
                    int s = score;
                    if (ip.startsWith("192.168.")) s += 30;
                    else if (ip.startsWith("10.")) s += 20;
                    else if (a.isSiteLocalAddress()) s += 10;
                    scored.put(ip, s);
                }
            }
        } catch (SocketException ignored) { }
        List<String> result = new ArrayList<>(scored.keySet());
        result.sort((x, y) -> Integer.compare(scored.get(y), scored.get(x)));
        if (result.isEmpty()) {
            try {
                InetAddress a = InetAddress.getLocalHost();
                if (!a.isLoopbackAddress() && a instanceof Inet4Address) result.add(a.getHostAddress());
            } catch (UnknownHostException ignored) { }
        }
        return result;
    }

    // ----------------------------------------------------------------- janitor
    static void sweep() {
        try {
            long now = System.currentTimeMillis();
            for (Map.Entry<String, PeerDevice> e : peers.entrySet()) {
                PeerDevice p = e.getValue();
                if (now - p.lastSeen > PEER_TTL_MS && peers.remove(e.getKey(), p)) {
                    log("[-] " + p.alias + " went offline");
                }
            }
            packets.removeIf(p -> now - p.ts > PACKET_TTL_MS);
            while (packets.size() > MAX_PACKETS) packets.remove(0);
        } catch (Throwable t) {
            // never let the scheduled task die
        }
    }

    static void log(String s) {
        synchronized (out) { out.println("[" + String.format("%tT", new Date()) + "] " + s); }
    }

    // ---------------------------------------------------------------- handlers
    static HttpHandler wrap(Route r) {
        return ex -> {
            try {
                r.handle(ex);
            } catch (Throwable t) {
                log("request failed: " + ex.getRequestURI().getPath() + " - " + t);
                try { respondJson(ex, 500, "{\"error\":\"server error\"}"); } catch (Throwable ignored) { }
            } finally {
                ex.close();
            }
        };
    }

    static void handleRoot(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        if (!"GET".equals(ex.getRequestMethod())) { methodNotAllowed(ex, "GET"); return; }
        if (!"/".equals(path) && !"/index.html".equals(path)) {
            respondJson(ex, 404, "{\"error\":\"not found\"}");
            return;
        }
        respond(ex, 200, "text/html; charset=utf-8", HTML.getBytes(StandardCharsets.UTF_8));
    }

    static void handleHeartbeat(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) { methodNotAllowed(ex, "POST"); return; }
        byte[] b = readLimited(ex.getRequestBody(), 4096);
        if (b == null) { respondJson(ex, 413, "{\"error\":\"too large\"}"); return; }
        String body = new String(b, StandardCharsets.UTF_8);
        String id = jsonString(body, "id");
        if (!validId(id)) { respondJson(ex, 400, "{\"error\":\"bad id\"}"); return; }
        String alias = cleanLabel(jsonString(body, "alias"), 32, "Unknown device");
        String type = jsonString(body, "type");
        if (!"phone".equals(type) && !"tablet".equals(type)) type = "desktop";

        final boolean[] fresh = {false};
        PeerDevice p = peers.computeIfAbsent(id, k -> { fresh[0] = true; return new PeerDevice(k); });
        p.alias = alias;
        p.type = type;
        p.ip = ex.getRemoteAddress().getAddress().getHostAddress();
        p.lastSeen = System.currentTimeMillis();
        if (fresh[0]) log("[+] " + alias + " (" + type + ") connected from " + p.ip);
        respondJson(ex, 200, "{\"status\":\"ok\"}");
    }

    static void handlePeers(HttpExchange ex) throws IOException {
        if (!"GET".equals(ex.getRequestMethod())) { methodNotAllowed(ex, "GET"); return; }
        List<PeerDevice> list = new ArrayList<>(peers.values());
        list.sort((a, b) -> a.alias.compareToIgnoreCase(b.alias));
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            PeerDevice p = list.get(i);
            if (i > 0) sb.append(',');
            sb.append("{\"id\":").append(q(p.id)).append(",\"alias\":").append(q(p.alias))
              .append(",\"type\":").append(q(p.type)).append('}');
        }
        respondJson(ex, 200, sb.append(']').toString());
    }

    static void handleSend(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) { methodNotAllowed(ex, "POST"); return; }
        Headers rh = ex.getRequestHeaders();

        String to = rh.getFirst("X-Target-To");
        if (to == null || to.trim().isEmpty() || "ALL".equalsIgnoreCase(to.trim())) to = "ALL";
        else if (validId(to.trim())) to = to.trim();
        else { drainAndFail(ex, 400, "bad X-Target-To"); return; }

        String alias = cleanLabel(urlDecode(rh.getFirst("X-Target-From")), 32, "Unknown device");
        String type = rh.getFirst("X-Packet-Type");
        type = type == null ? "" : type.trim().toLowerCase();
        String fromId = rh.getFirst("X-Sender-Id");
        if (!validId(fromId)) fromId = "";
        long now = System.currentTimeMillis();
        String pid = Long.toString(seq.incrementAndGet());
        TransferPacket packet;

        if ("text".equals(type)) {
            byte[] b = readLimited(ex.getRequestBody(), MAX_TEXT_BYTES);
            if (b == null) { respondJson(ex, 413, "{\"error\":\"text too large (max 1 MB)\"}"); return; }
            String text = new String(b, StandardCharsets.UTF_8);
            if (text.trim().isEmpty()) { respondJson(ex, 400, "{\"error\":\"empty text\"}"); return; }
            packet = new TransferPacket(pid, fromId, alias, to, "text", text, null, b.length, now);
            log("[>] " + alias + " -> " + ("ALL".equals(to) ? "everyone" : to) + " : text (" + b.length + " bytes)");
        } else if ("file".equals(type)) {
            String name = sanitizeFileName(urlDecode(rh.getFirst("X-File-Name")));
            long declared = -1;
            try { String cl = rh.getFirst("Content-Length"); if (cl != null) declared = Long.parseLong(cl.trim()); }
            catch (NumberFormatException ignored) { }
            if (declared > MAX_FILE_BYTES) { respondJson(ex, 413, "{\"error\":\"file too large (max 2 GB)\"}"); return; }

            File dest = createUniqueFile(name);
            long size = 0;
            boolean ok = false, tooLarge = false;
            try (InputStream in = ex.getRequestBody();
                 OutputStream fo = new BufferedOutputStream(new FileOutputStream(dest), 1 << 16)) {
                byte[] buf = new byte[1 << 16];
                int n;
                while ((n = in.read(buf)) != -1) {
                    size += n;
                    if (size > MAX_FILE_BYTES) { tooLarge = true; break; }
                    fo.write(buf, 0, n);
                }
                ok = !tooLarge;
            } finally {
                if (!ok) dest.delete();
            }
            if (tooLarge) { respondJson(ex, 413, "{\"error\":\"file too large (max 2 GB)\"}"); return; }
            if (declared >= 0 && size != declared) {
                dest.delete();
                respondJson(ex, 400, "{\"error\":\"upload incomplete\"}");
                return;
            }
            packet = new TransferPacket(pid, fromId, alias, to, "file", null, dest.getName(), size, now);
            log("[>] " + alias + " -> " + ("ALL".equals(to) ? "everyone" : to) + " : file " + dest.getName() + " (" + humanSize(size) + ")");
        } else {
            drainAndFail(ex, 400, "X-Packet-Type must be text or file");
            return;
        }

        packets.add(packet);
        respondJson(ex, 200, "{\"status\":\"ok\",\"id\":" + q(pid) + "}");
    }

    static void handleInbox(HttpExchange ex) throws IOException {
        if (!"GET".equals(ex.getRequestMethod())) { methodNotAllowed(ex, "GET"); return; }
        String peerId = query(ex).get("peerId");
        if (!validId(peerId)) { respondJson(ex, 400, "{\"error\":\"bad peerId\"}"); return; }
        List<TransferPacket> mine = new ArrayList<>();
        for (TransferPacket p : packets) {
            if (p.fromId.equals(peerId)) continue;                        // don't echo your own sends back
            if ("ALL".equals(p.toId) || p.toId.equals(peerId)) mine.add(p);
        }
        int from = Math.max(0, mine.size() - 200);
        StringBuilder sb = new StringBuilder("[");
        for (int i = from; i < mine.size(); i++) {
            if (i > from) sb.append(',');
            sb.append(mine.get(i).toJson());
        }
        respondJson(ex, 200, sb.append(']').toString());
    }

    static void handleDownload(HttpExchange ex) throws IOException {
        if (!"GET".equals(ex.getRequestMethod())) { methodNotAllowed(ex, "GET"); return; }
        Map<String, String> qs = query(ex);
        String name = qs.get("file");
        if (name == null || name.isEmpty() || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0 || name.indexOf('\0') >= 0) {
            respondJson(ex, 400, "{\"error\":\"bad file name\"}");
            return;
        }
        File canon;
        try {
            canon = new File(dropDir, name).getCanonicalFile();
        } catch (IOException e) {
            respondJson(ex, 400, "{\"error\":\"bad file name\"}");
            return;
        }
        // Path canonicalization: the resolved file must live directly inside shared_drops.
        if (canon.getParentFile() == null || !canon.getParentFile().equals(dropDirCanon)) {
            respondJson(ex, 403, "{\"error\":\"forbidden\"}");
            return;
        }
        if (!canon.isFile()) { respondJson(ex, 404, "{\"error\":\"file not found\"}"); return; }

        String lower = canon.getName().toLowerCase();
        String imageType = null;
        if (lower.endsWith(".png")) imageType = "image/png";
        else if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) imageType = "image/jpeg";
        else if (lower.endsWith(".gif")) imageType = "image/gif";
        else if (lower.endsWith(".webp")) imageType = "image/webp";
        else if (lower.endsWith(".bmp")) imageType = "image/bmp";
        boolean inline = "1".equals(qs.get("inline")) && imageType != null;

        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", inline ? imageType : "application/octet-stream");
        h.set("Content-Disposition", contentDisposition(inline ? "inline" : "attachment", canon.getName()));
        h.set("X-Content-Type-Options", "nosniff");
        h.set("Cache-Control", "no-store");
        long len = canon.length();
        ex.sendResponseHeaders(200, len == 0 ? -1 : len);
        if (len > 0) {
            try (InputStream in = new FileInputStream(canon); OutputStream os = ex.getResponseBody()) {
                byte[] buf = new byte[1 << 16];
                int n;
                while ((n = in.read(buf)) != -1) os.write(buf, 0, n);
            }
        }
    }

    static void handleInfo(HttpExchange ex) throws IOException {
        if (!"GET".equals(ex.getRequestMethod())) { methodNotAllowed(ex, "GET"); return; }
        StringBuilder sb = new StringBuilder("{\"url\":").append(q(primaryUrl)).append(",\"urls\":[");
        List<String> urls = allUrls;
        for (int i = 0; i < urls.size(); i++) { if (i > 0) sb.append(','); sb.append(q(urls.get(i))); }
        sb.append("],\"maxFileBytes\":").append(MAX_FILE_BYTES).append('}');
        respondJson(ex, 200, sb.toString());
    }

    static void handleQr(HttpExchange ex) throws IOException {
        if (!"GET".equals(ex.getRequestMethod())) { methodNotAllowed(ex, "GET"); return; }
        QrCode qr = QrCode.encode(primaryUrl);
        respond(ex, 200, "image/svg+xml", qr.toSvg(4).getBytes(StandardCharsets.UTF_8));
    }

    // ----------------------------------------------------------------- helpers
    static void respond(HttpExchange ex, int code, String ctype, byte[] body) throws IOException {
        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", ctype);
        h.set("Cache-Control", "no-store");
        h.set("X-Content-Type-Options", "nosniff");
        if (body.length == 0) { ex.sendResponseHeaders(code, -1); return; }
        ex.sendResponseHeaders(code, body.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(body); }
    }

    static void respondJson(HttpExchange ex, int code, String json) throws IOException {
        respond(ex, code, "application/json; charset=utf-8", json.getBytes(StandardCharsets.UTF_8));
    }

    static void methodNotAllowed(HttpExchange ex, String allow) throws IOException {
        ex.getResponseHeaders().set("Allow", allow);
        respondJson(ex, 405, "{\"error\":\"method not allowed\"}");
    }

    /** Read (and discard) up to 1 MB of the body so the client sees our error instead of a reset. */
    static void drainAndFail(HttpExchange ex, int code, String msg) throws IOException {
        readLimited(ex.getRequestBody(), 1 << 20);
        respondJson(ex, code, "{\"error\":" + q(msg) + "}");
    }

    static byte[] readLimited(InputStream in, int limit) throws IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        long total = 0;
        while ((n = in.read(buf)) != -1) {
            total += n;
            if (total > limit) return null;
            bo.write(buf, 0, n);
        }
        return bo.toByteArray();
    }

    static Map<String, String> query(HttpExchange ex) {
        Map<String, String> m = new HashMap<>();
        String qs = ex.getRequestURI().getRawQuery();
        if (qs == null) return m;
        for (String part : qs.split("&")) {
            if (part.isEmpty()) continue;
            int i = part.indexOf('=');
            String k = i < 0 ? part : part.substring(0, i);
            String v = i < 0 ? "" : part.substring(i + 1);
            try { m.put(URLDecoder.decode(k, StandardCharsets.UTF_8), URLDecoder.decode(v, StandardCharsets.UTF_8)); }
            catch (IllegalArgumentException ignored) { }
        }
        return m;
    }

    static String urlDecode(String s) {
        if (s == null) return null;
        try { return URLDecoder.decode(s, StandardCharsets.UTF_8); } catch (IllegalArgumentException e) { return s; }
    }

    static boolean validId(String s) {
        if (s == null || s.length() < 4 || s.length() > 64) return false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_';
            if (!ok) return false;
        }
        return true;
    }

    static String cleanLabel(String s, int max, String fallback) {
        if (s == null) return fallback;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= 32 && c != 127) sb.append(c);
        }
        String r = sb.toString().trim();
        if (r.length() > max) r = r.substring(0, max).trim();
        return r.isEmpty() ? fallback : r;
    }

    static String sanitizeFileName(String raw) {
        if (raw == null) raw = "";
        int slash = Math.max(raw.lastIndexOf('/'), raw.lastIndexOf('\\'));
        if (slash >= 0) raw = raw.substring(slash + 1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c < 32 || c == 127 || "<>:\"|?*".indexOf(c) >= 0) sb.append('_'); else sb.append(c);
        }
        String s = sb.toString().trim();
        while (s.startsWith(".")) s = s.substring(1);
        while (s.endsWith(".") || s.endsWith(" ")) s = s.substring(0, s.length() - 1);
        if (s.length() > 150) {
            int dot = s.lastIndexOf('.');
            String ext = (dot > 0 && s.length() - dot <= 10) ? s.substring(dot) : "";
            s = s.substring(0, 150 - ext.length()) + ext;
        }
        if (s.isEmpty()) s = "file";
        int dot = s.indexOf('.');
        String stem = (dot >= 0 ? s.substring(0, dot) : s).toUpperCase();
        if (RESERVED.contains(stem)) s = "_" + s;
        return s;
    }

    static File createUniqueFile(String name) throws IOException {
        int dot = name.lastIndexOf('.');
        String stem = dot > 0 ? name.substring(0, dot) : name;
        String ext = dot > 0 ? name.substring(dot) : "";
        for (int i = 0; i < 10000; i++) {
            String cand = i == 0 ? name : stem + " (" + i + ")" + ext;
            File f = new File(dropDir, cand);
            if (f.createNewFile()) return f;        // atomic: two simultaneous uploads never share a name
        }
        throw new IOException("cannot allocate a file name");
    }

    static String contentDisposition(String kind, String name) {
        StringBuilder ascii = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            ascii.append(c >= 32 && c < 127 && c != '"' && c != '\\' && c != ';' && c != '%' ? c : '_');
        }
        String enc = URLEncoder.encode(name, StandardCharsets.UTF_8).replace("+", "%20").replace("*", "%2A");
        return kind + "; filename=\"" + ascii + "\"; filename*=UTF-8''" + enc;
    }

    static String humanSize(long n) {
        if (n < 1024) return n + " B";
        String[] u = {"KB", "MB", "GB", "TB"};
        double d = n;
        int i = -1;
        do { d /= 1024; i++; } while (d >= 1024 && i < u.length - 1);
        return String.format(Locale.ROOT, "%.1f %s", d, u[i]);
    }

    // minimal JSON helpers (no libraries)
    static String q(String s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 32 || c == '\u2028' || c == '\u2029') sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        return sb.append('"').toString();
    }

    static String jsonString(String json, String key) {
        String needle = "\"" + key + "\"";
        int i = json.indexOf(needle);
        if (i < 0) return null;
        i += needle.length();
        int len = json.length();
        while (i < len && Character.isWhitespace(json.charAt(i))) i++;
        if (i >= len || json.charAt(i) != ':') return null;
        i++;
        while (i < len && Character.isWhitespace(json.charAt(i))) i++;
        if (i >= len || json.charAt(i) != '"') return null;
        i++;
        StringBuilder sb = new StringBuilder();
        while (i < len) {
            char c = json.charAt(i++);
            if (c == '"') return sb.toString();
            if (c == '\\' && i < len) {
                char e = json.charAt(i++);
                switch (e) {
                    case 'n': sb.append('\n'); break;
                    case 't': sb.append('\t'); break;
                    case 'r': sb.append('\r'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case 'u':
                        if (i + 4 <= len) {
                            try { sb.append((char) Integer.parseInt(json.substring(i, i + 4), 16)); }
                            catch (NumberFormatException ignored) { }
                            i += 4;
                        }
                        break;
                    default: sb.append(e);
                }
            } else sb.append(c);
        }
        return null;
    }

    // ================================================================ web UI
        static final String HTML = buildHtml();

    /** The whole web UI, appended line by line (works on JDK 11; text blocks need JDK 15+). */
    static String buildHtml() {
        StringBuilder b = new StringBuilder(40000);
        b.append("<!DOCTYPE html>\n");
        b.append("<html lang=\"en\">\n");
        b.append("<head>\n");
        b.append("<meta charset=\"utf-8\">\n");
        b.append("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1,viewport-fit=cover\">\n");
        b.append("<meta name=\"theme-color\" content=\"#0b0f19\">\n");
        b.append("<title>ClipFlow</title>\n");
        b.append("<link rel=\"icon\" href=\"data:,\">\n");
        b.append("<style>\n");
        b.append(":root{--bg:#0b0f19;--panel:#141c2e;--panel2:#10182a;--border:#222f49;--green:#22c55e;--green-soft:rgba(34,197,94,.12);--blue:#38bdf8;--blue-soft:rgba(56,189,248,.12);--text:#e7ecf6;--muted:#8a97b3;--red:#f87171;--amber:#fbbf24}\n");
        b.append("*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}\n");
        b.append("html{background:var(--bg)}\n");
        b.append("body{margin:0;min-height:100vh;background:var(--bg);color:var(--text);font:16px/1.45 -apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,Helvetica,Arial,sans-serif;-webkit-text-size-adjust:100%}\n");
        b.append(".wrap{max-width:760px;margin:0 auto;padding:16px 14px 48px;padding-left:max(14px,env(safe-area-inset-left));padding-right:max(14px,env(safe-area-inset-right))}\n");
        b.append(".card{background:var(--panel);border:1px solid var(--border);border-radius:16px}\n");
        b.append(".top{display:flex;align-items:center;gap:12px;padding:14px 16px;margin-bottom:12px}\n");
        b.append(".avatar{width:48px;height:48px;border-radius:14px;background:var(--green-soft);display:grid;place-items:center;font-size:25px;flex:none}\n");
        b.append(".who{min-width:0;flex:1}\n");
        b.append(".alias{background:none;border:0;color:var(--text);font:inherit;font-weight:650;font-size:18px;padding:0;cursor:pointer;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:left;display:block}\n");
        b.append(".sub{color:var(--muted);font-size:13px}\n");
        b.append(".pill{display:inline-flex;align-items:center;gap:8px;margin-top:4px;padding:4px 11px;border-radius:999px;background:var(--green-soft);color:var(--green);font-size:13px;font-weight:600;white-space:nowrap}\n");
        b.append(".dot{width:9px;height:9px;border-radius:50%;background:var(--green);animation:pulse 1.8s infinite}\n");
        b.append(".pill.warn{background:rgba(251,191,36,.12);color:var(--amber)}\n");
        b.append(".pill.warn .dot{background:var(--amber);animation:none}\n");
        b.append(".pill.off{background:rgba(248,113,113,.12);color:var(--red)}\n");
        b.append(".pill.off .dot{background:var(--red);animation:none}\n");
        b.append("@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(34,197,94,.55)}70%{box-shadow:0 0 0 9px rgba(34,197,94,0)}100%{box-shadow:0 0 0 0 rgba(34,197,94,0)}}\n");
        b.append(".radar{padding:14px 0 12px 16px;margin-bottom:12px}\n");
        b.append(".rhead{display:flex;align-items:center;gap:10px;padding-right:16px;margin-bottom:10px}\n");
        b.append(".rtitle{font-weight:650}\n");
        b.append(".rcount{color:var(--muted);font-size:13px;margin-left:auto}\n");
        b.append(".ping{position:relative;width:12px;height:12px;flex:none}\n");
        b.append(".ping:before,.ping:after{content:\"\";position:absolute;left:0;top:0;right:0;bottom:0;border-radius:50%;border:2px solid var(--green)}\n");
        b.append(".ping:after{animation:ring 2s infinite}\n");
        b.append("@keyframes ring{from{transform:scale(1);opacity:.9}to{transform:scale(2.6);opacity:0}}\n");
        b.append(".tray{display:flex;gap:10px;overflow-x:auto;padding:2px 16px 6px 0;scroll-snap-type:x proximity;-webkit-overflow-scrolling:touch}\n");
        b.append(".chip{flex:none;scroll-snap-align:start;display:flex;align-items:center;gap:10px;padding:9px 16px 9px 9px;background:var(--panel2);border:1px solid var(--border);color:var(--text);border-radius:14px;font:inherit;cursor:pointer;text-align:left;min-width:150px}\n");
        b.append(".chip .ic{width:36px;height:36px;border-radius:11px;background:var(--green-soft);display:grid;place-items:center;font-size:19px;flex:none}\n");
        b.append(".chip b{display:block;font-size:14px;font-weight:600;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n");
        b.append(".chip small{color:var(--muted);font-size:12px}\n");
        b.append(".chip.active{border-color:var(--blue);background:var(--blue-soft);box-shadow:0 0 0 1px var(--blue) inset}\n");
        b.append(".hint{color:var(--muted);font-size:13px;padding:8px 16px 0 0}\n");
        b.append(".tabs{display:flex;gap:6px;padding:5px;margin-bottom:10px}\n");
        b.append(".tab{flex:1;background:none;border:0;color:var(--muted);font:inherit;font-weight:600;font-size:15px;padding:11px 6px;border-radius:12px;cursor:pointer;white-space:nowrap}\n");
        b.append(".tab.on{background:var(--blue-soft);color:var(--blue)}\n");
        b.append(".nbadge{display:inline-block;min-width:20px;padding:0 6px;margin-left:6px;border-radius:999px;background:var(--green);color:#04210f;font-size:12px;font-weight:700;line-height:20px;text-align:center}\n");
        b.append(".dest{display:flex;align-items:center;gap:8px;padding:10px 14px;margin-bottom:10px;font-size:14px;color:var(--muted)}\n");
        b.append(".dest b{color:var(--blue);font-weight:650}\n");
        b.append(".panel{padding:16px}\n");
        b.append("textarea{width:100%;min-height:150px;resize:vertical;background:var(--panel2);color:var(--text);border:1px solid var(--border);border-radius:12px;padding:12px 14px;font:16px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;outline:none}\n");
        b.append("textarea:focus{border-color:var(--blue)}\n");
        b.append(".row{display:flex;gap:8px;margin-top:12px;flex-wrap:wrap;align-items:center}\n");
        b.append(".btn{border:1px solid var(--border);background:var(--panel2);color:var(--text);font:inherit;font-weight:600;font-size:15px;padding:10px 16px;border-radius:12px;cursor:pointer;text-decoration:none;display:inline-block}\n");
        b.append(".btn.primary{background:var(--blue);border-color:var(--blue);color:#04202e}\n");
        b.append(".btn.small{padding:7px 12px;font-size:14px}\n");
        b.append(".btn:active{transform:scale(.97)}\n");
        b.append(".btn[disabled]{opacity:.5;cursor:default}\n");
        b.append(".grow{flex:1}\n");
        b.append(".dz{display:block;border:2px dashed var(--border);border-radius:14px;padding:32px 16px;text-align:center;cursor:pointer;color:var(--muted);background:var(--panel2)}\n");
        b.append(".dz.over{border-color:var(--green);background:var(--green-soft);color:var(--green)}\n");
        b.append(".dz big{display:block;font-size:34px;margin-bottom:6px}\n");
        b.append(".dz b{color:var(--text)}\n");
        b.append(".ups{margin-top:12px;display:flex;flex-direction:column;gap:8px}\n");
        b.append(".up{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:var(--panel2);font-size:14px}\n");
        b.append(".up .n{display:flex;gap:8px;justify-content:space-between}\n");
        b.append(".up .n span:first-child{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n");
        b.append(".bar{height:6px;border-radius:99px;background:var(--border);margin-top:8px;overflow:hidden}\n");
        b.append(".bar i{display:block;height:100%;width:0;background:var(--blue);transition:width .15s}\n");
        b.append(".up.ok .bar i{background:var(--green)}\n");
        b.append(".up.bad .bar i{background:var(--red)}\n");
        b.append(".st{color:var(--muted);font-size:13px;margin-top:4px}\n");
        b.append(".up.bad .st{color:var(--red)}\n");
        b.append(".feed{display:flex;flex-direction:column;gap:10px}\n");
        b.append(".item{padding:14px}\n");
        b.append(".ihead{display:flex;align-items:center;gap:8px;margin-bottom:8px;flex-wrap:wrap}\n");
        b.append(".from{font-weight:650}\n");
        b.append(".badge{font-size:12px;font-weight:650;padding:2px 9px;border-radius:99px}\n");
        b.append(".badge.direct{background:var(--blue-soft);color:var(--blue)}\n");
        b.append(".badge.bcast{background:var(--green-soft);color:var(--green)}\n");
        b.append(".time{margin-left:auto;color:var(--muted);font-size:13px}\n");
        b.append(".txt{margin:0 0 10px;padding:10px 12px;background:var(--panel2);border:1px solid var(--border);border-radius:10px;font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;white-space:pre-wrap;word-break:break-word;max-height:240px;overflow:auto}\n");
        b.append(".file{display:flex;align-items:center;gap:12px}\n");
        b.append(".file .fi{width:44px;height:44px;border-radius:12px;background:var(--blue-soft);display:grid;place-items:center;font-size:22px;flex:none}\n");
        b.append(".file .fn{min-width:0;flex:1}\n");
        b.append(".file .fn b{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n");
        b.append(".thumb{display:block;max-width:100%;max-height:200px;border-radius:10px;margin:0 0 10px;object-fit:contain;background:var(--panel2)}\n");
        b.append(".empty{text-align:center;color:var(--muted);padding:34px 12px}\n");
        b.append(".empty big{display:block;font-size:34px;margin-bottom:6px}\n");
        b.append(".toasts{position:fixed;left:0;right:0;bottom:18px;display:flex;flex-direction:column;align-items:center;gap:8px;pointer-events:none;z-index:50}\n");
        b.append(".toast{background:#1d2a47;border:1px solid var(--border);color:var(--text);padding:10px 16px;border-radius:12px;font-size:14px;box-shadow:0 8px 24px rgba(0,0,0,.45);max-width:90vw}\n");
        b.append(".modal{position:fixed;left:0;top:0;right:0;bottom:0;background:rgba(5,8,15,.78);display:flex;align-items:center;justify-content:center;padding:16px;z-index:60}\n");
        b.append(".modal[hidden]{display:none}\n");
        b.append(".mbox{padding:20px;max-width:360px;width:100%;text-align:center}\n");
        b.append(".mbox h3{margin:0 0 4px}\n");
        b.append(".mbox img{width:240px;height:240px;max-width:100%;border-radius:12px;margin:14px auto;display:block;background:#fff}\n");
        b.append(".mbox .u{font:14px ui-monospace,Menlo,Consolas,monospace;color:var(--blue);word-break:break-all;margin-bottom:4px}\n");
        b.append("[hidden]{display:none!important}\n");
        b.append("</style>\n");
        b.append("</head>\n");
        b.append("<body>\n");
        b.append("<div class=\"wrap\">\n");
        b.append("  <div class=\"card top\">\n");
        b.append("    <div class=\"avatar\" id=\"avatar\">&#128187;</div>\n");
        b.append("    <div class=\"who\">\n");
        b.append("      <button class=\"alias\" id=\"alias\" title=\"Tap to rename this device\">...</button>\n");
        b.append("      <div class=\"pill\" id=\"pill\"><span class=\"dot\"></span><span id=\"pilltxt\">Online on Local Wi-Fi</span></div>\n");
        b.append("    </div>\n");
        b.append("    <button class=\"btn small\" id=\"pairBtn\">Pair device</button>\n");
        b.append("  </div>\n");
        b.append("\n");
        b.append("  <div class=\"card radar\">\n");
        b.append("    <div class=\"rhead\"><span class=\"ping\"></span><span class=\"rtitle\">Nearby Radar</span><span class=\"rcount\" id=\"rcount\">0 nearby</span></div>\n");
        b.append("    <div class=\"tray\" id=\"tray\"></div>\n");
        b.append("    <div class=\"hint\" id=\"rhint\">Open ClipFlow on another device on this Wi-Fi and it will appear here.</div>\n");
        b.append("  </div>\n");
        b.append("\n");
        b.append("  <div class=\"card tabs\">\n");
        b.append("    <button class=\"tab on\" data-tab=\"text\">Send Text</button>\n");
        b.append("    <button class=\"tab\" data-tab=\"files\">Send Files</button>\n");
        b.append("    <button class=\"tab\" data-tab=\"inbox\">Inbox / Feed<span class=\"nbadge\" id=\"nbadge\" hidden>0</span></button>\n");
        b.append("  </div>\n");
        b.append("\n");
        b.append("  <div class=\"card dest\" id=\"dest\"></div>\n");
        b.append("\n");
        b.append("  <div class=\"card panel\" id=\"p-text\">\n");
        b.append("    <textarea id=\"ta\" placeholder=\"Paste a link, OTP, terminal command or note...\" spellcheck=\"false\" autocapitalize=\"off\"></textarea>\n");
        b.append("    <div class=\"row\">\n");
        b.append("      <button class=\"btn primary\" id=\"sendText\">Send</button>\n");
        b.append("      <button class=\"btn\" id=\"pasteBtn\" hidden>Paste</button>\n");
        b.append("      <button class=\"btn\" id=\"clearText\">Clear</button>\n");
        b.append("      <span class=\"sub grow\" id=\"charcount\" style=\"text-align:right\"></span>\n");
        b.append("    </div>\n");
        b.append("  </div>\n");
        b.append("\n");
        b.append("  <div class=\"card panel\" id=\"p-files\" hidden>\n");
        b.append("    <label class=\"dz\" id=\"dz\" for=\"fileInput\">\n");
        b.append("      <big>&#128193;</big>\n");
        b.append("      <b>Drop files here</b> or tap to choose<br>\n");
        b.append("      <span class=\"sub\">Images, PDFs, documents, source code - anything. You can also paste a screenshot.</span>\n");
        b.append("    </label>\n");
        b.append("    <input type=\"file\" id=\"fileInput\" multiple hidden>\n");
        b.append("    <div class=\"ups\" id=\"ups\"></div>\n");
        b.append("  </div>\n");
        b.append("\n");
        b.append("  <div id=\"p-inbox\" hidden>\n");
        b.append("    <div class=\"row\" style=\"margin:0 0 10px\"><span class=\"sub grow\">Items sent to this device or broadcast to everyone.</span><button class=\"btn small\" id=\"clearFeed\">Clear</button></div>\n");
        b.append("    <div class=\"feed\" id=\"feed\"></div>\n");
        b.append("  </div>\n");
        b.append("</div>\n");
        b.append("\n");
        b.append("<div class=\"modal\" id=\"modal\" hidden>\n");
        b.append("  <div class=\"card mbox\">\n");
        b.append("    <h3>Pair another device</h3>\n");
        b.append("    <div class=\"sub\">Scan with the phone camera (same Wi-Fi) or type the address.</div>\n");
        b.append("    <img id=\"qrimg\" alt=\"QR code to open ClipFlow\" width=\"240\" height=\"240\">\n");
        b.append("    <div id=\"urls\"></div>\n");
        b.append("    <div class=\"row\" style=\"justify-content:center\"><button class=\"btn\" id=\"mclose\">Close</button></div>\n");
        b.append("  </div>\n");
        b.append("</div>\n");
        b.append("<div class=\"toasts\" id=\"toasts\"></div>\n");
        b.append("<script>\n");
        b.append("(function(){\n");
        b.append("'use strict';\n");
        b.append("var ALL='ALL';\n");
        b.append("function $(s){return document.querySelector(s);}\n");
        b.append("function el(tag,cls,text){var e=document.createElement(tag);if(cls)e.className=cls;if(text!=null)e.textContent=text;return e;}\n");
        b.append("function cp(n){return String.fromCodePoint(n);}\n");
        b.append("var ICON={desktop:cp(0x1F4BB),phone:cp(0x1F4F1),tablet:cp(0x1F4DF)};\n");
        b.append("var TYPE_NAME={desktop:'Desktop',phone:'Phone',tablet:'Tablet'};\n");
        b.append("function lsGet(k){try{return localStorage.getItem(k);}catch(e){return null;}}\n");
        b.append("function lsSet(k,v){try{localStorage.setItem(k,v);}catch(e){}}\n");
        b.append("function ssGet(k){try{return sessionStorage.getItem(k);}catch(e){return null;}}\n");
        b.append("function ssSet(k,v){try{sessionStorage.setItem(k,v);}catch(e){}}\n");
        b.append("\n");
        b.append("var state={id:null,alias:'',type:'desktop',tabScoped:false,target:ALL,peers:[],peerSig:null,\n");
        b.append("  feedSig:null,seen:{},firstLoad:true,unread:0,tab:'text',lastBeatOk:0,maxFile:2147483648,\n");
        b.append("  cleared:0,maxTs:0,urls:[],chain:Promise.resolve()};\n");
        b.append("\n");
        b.append("/* ---------- identity ---------- */\n");
        b.append("function detectType(){\n");
        b.append("  var ua=navigator.userAgent||'';var touch=navigator.maxTouchPoints||0;\n");
        b.append("  if(ua.indexOf('iPad')>=0||(ua.indexOf('Macintosh')>=0&&touch>1))return 'tablet';\n");
        b.append("  if(ua.indexOf('iPhone')>=0||ua.indexOf('iPod')>=0)return 'phone';\n");
        b.append("  if(ua.indexOf('Android')>=0)return ua.indexOf('Mobile')>=0?'phone':'tablet';\n");
        b.append("  if(ua.indexOf('Tablet')>=0)return 'tablet';\n");
        b.append("  return 'desktop';\n");
        b.append("}\n");
        b.append("function rid(){\n");
        b.append("  var s='';\n");
        b.append("  try{var a=new Uint8Array(8);window.crypto.getRandomValues(a);for(var i=0;i<a.length;i++){s+=('0'+a[i].toString(16)).slice(-2);}}\n");
        b.append("  catch(e){for(var j=0;j<16;j++){s+=Math.floor(Math.random()*16).toString(16);}}\n");
        b.append("  return 'd'+s;\n");
        b.append("}\n");
        b.append("var COLORS=['Emerald','Sapphire','Ruby','Amber','Violet','Coral','Topaz','Onyx','Jade','Indigo','Crimson','Cobalt','Pearl','Ivory'];\n");
        b.append("var NOUNS={phone:['Phone'],tablet:['Tablet'],desktop:['Laptop','Desktop']};\n");
        b.append("function makeAlias(){\n");
        b.append("  var n=NOUNS[state.type]||NOUNS.desktop;\n");
        b.append("  return COLORS[Math.floor(Math.random()*COLORS.length)]+' '+n[Math.floor(Math.random()*n.length)];\n");
        b.append("}\n");
        b.append("function initIdentity(done){\n");
        b.append("  state.type=detectType();\n");
        b.append("  var sid=ssGet('cf_tab_id');\n");
        b.append("  if(sid){state.id=sid;state.alias=ssGet('cf_tab_alias')||makeAlias();state.tabScoped=true;return done();}\n");
        b.append("  var base=lsGet('cf_id');if(!base){base=rid();lsSet('cf_id',base);}\n");
        b.append("  var alias=lsGet('cf_alias');if(!alias){alias=makeAlias();lsSet('cf_alias',alias);}\n");
        b.append("  if(!window.BroadcastChannel){state.id=base;state.alias=alias;return done();}\n");
        b.append("  /* two tabs of one browser share localStorage; give the newer tab its own identity so both show up */\n");
        b.append("  var ch=new BroadcastChannel('clipflow'),clash=false,myId=null,nonce=rid();\n");
        b.append("  ch.onmessage=function(e){\n");
        b.append("    var m=e.data||{};\n");
        b.append("    if(m.t==='who'&&myId)ch.postMessage({t:'iam',id:myId,nonce:m.nonce});\n");
        b.append("    if(m.t==='iam'&&m.nonce===nonce&&m.id===base)clash=true;\n");
        b.append("  };\n");
        b.append("  ch.postMessage({t:'who',nonce:nonce});\n");
        b.append("  setTimeout(function(){\n");
        b.append("    if(clash){state.id=rid();state.alias=makeAlias();state.tabScoped=true;ssSet('cf_tab_id',state.id);ssSet('cf_tab_alias',state.alias);}\n");
        b.append("    else{state.id=base;state.alias=alias;}\n");
        b.append("    myId=state.id;done();\n");
        b.append("  },250);\n");
        b.append("}\n");
        b.append("\n");
        b.append("/* ---------- ui helpers ---------- */\n");
        b.append("function toast(msg){\n");
        b.append("  var t=el('div','toast',msg);$('#toasts').appendChild(t);\n");
        b.append("  setTimeout(function(){if(t.parentNode)t.parentNode.removeChild(t);},2600);\n");
        b.append("}\n");
        b.append("function fmtSize(n){\n");
        b.append("  if(n<1024)return n+' B';\n");
        b.append("  var u=['KB','MB','GB','TB'],i=-1;\n");
        b.append("  do{n/=1024;i++;}while(n>=1024&&i<u.length-1);\n");
        b.append("  return (n>=100?n.toFixed(0):n.toFixed(1))+' '+u[i];\n");
        b.append("}\n");
        b.append("function fmtTime(ts){try{return new Date(ts).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});}catch(e){return '';}}\n");
        b.append("function extOf(name){var n=name.toLowerCase(),d=n.lastIndexOf('.');return d>=0?n.slice(d+1):'';}\n");
        b.append("function isImage(name){return ['png','jpg','jpeg','gif','webp','bmp'].indexOf(extOf(name))>=0;}\n");
        b.append("function fileIcon(name){\n");
        b.append("  var e=extOf(name);\n");
        b.append("  if(isImage(name))return cp(0x1F5BC);\n");
        b.append("  if(e==='pdf')return cp(0x1F4D5);\n");
        b.append("  if(['zip','rar','7z','gz','tar'].indexOf(e)>=0)return cp(0x1F4E6);\n");
        b.append("  if(['mp3','wav','flac','m4a'].indexOf(e)>=0)return cp(0x1F3B5);\n");
        b.append("  if(['mp4','mov','mkv','webm','avi'].indexOf(e)>=0)return cp(0x1F3AC);\n");
        b.append("  return cp(0x1F4C4);\n");
        b.append("}\n");
        b.append("function isHttpUrl(t){\n");
        b.append("  t=t.trim();\n");
        b.append("  if(t.indexOf('http://')!==0&&t.indexOf('https://')!==0)return false;\n");
        b.append("  return t.indexOf(' ')<0&&t.indexOf(String.fromCharCode(10))<0;\n");
        b.append("}\n");
        b.append("function legacyCopy(t){\n");
        b.append("  return new Promise(function(res,rej){\n");
        b.append("    var ta=document.createElement('textarea');\n");
        b.append("    ta.value=t;ta.setAttribute('readonly','');\n");
        b.append("    ta.style.cssText='position:fixed;top:0;left:0;opacity:0;font-size:16px';\n");
        b.append("    document.body.appendChild(ta);ta.focus();ta.select();\n");
        b.append("    try{ta.setSelectionRange(0,t.length);}catch(e){}\n");
        b.append("    var ok=false;try{ok=document.execCommand('copy');}catch(e2){}\n");
        b.append("    document.body.removeChild(ta);\n");
        b.append("    if(ok)res();else rej(new Error('copy failed'));\n");
        b.append("  });\n");
        b.append("}\n");
        b.append("/* navigator.clipboard only works on https/localhost; plain http://192.168.x.x needs the legacy path */\n");
        b.append("function copyText(t){\n");
        b.append("  if(navigator.clipboard&&window.isSecureContext){\n");
        b.append("    return navigator.clipboard.writeText(t).catch(function(){return legacyCopy(t);});\n");
        b.append("  }\n");
        b.append("  return legacyCopy(t);\n");
        b.append("}\n");
        b.append("function destName(){\n");
        b.append("  if(state.target===ALL)return 'All Devices (Broadcast)';\n");
        b.append("  for(var i=0;i<state.peers.length;i++)if(state.peers[i].id===state.target)return state.peers[i].alias;\n");
        b.append("  return 'Unknown device';\n");
        b.append("}\n");
        b.append("\n");
        b.append("/* ---------- header / status ---------- */\n");
        b.append("function renderHeader(){\n");
        b.append("  $('#avatar').textContent=ICON[state.type]||ICON.desktop;\n");
        b.append("  $('#alias').textContent=state.alias;\n");
        b.append("  document.title='ClipFlow - '+state.alias;\n");
        b.append("}\n");
        b.append("function setStatus(){\n");
        b.append("  var age=Date.now()-state.lastBeatOk,p=$('#pill'),t=$('#pilltxt');\n");
        b.append("  if(age<6000){p.className='pill';t.textContent='Online on Local Wi-Fi';}\n");
        b.append("  else if(age<15000){p.className='pill warn';t.textContent='Reconnecting...';}\n");
        b.append("  else{p.className='pill off';t.textContent='Offline - server unreachable';}\n");
        b.append("}\n");
        b.append("$('#alias').addEventListener('click',function(){\n");
        b.append("  var v=window.prompt('Rename this device',state.alias);\n");
        b.append("  if(v==null)return;\n");
        b.append("  v=v.trim().slice(0,32);if(!v)return;\n");
        b.append("  state.alias=v;\n");
        b.append("  if(state.tabScoped)ssSet('cf_tab_alias',v);else lsSet('cf_alias',v);\n");
        b.append("  renderHeader();beat();\n");
        b.append("});\n");
        b.append("\n");
        b.append("/* ---------- heartbeat + polling ---------- */\n");
        b.append("function getJson(url){\n");
        b.append("  return fetch(url,{cache:'no-store'}).then(function(r){if(!r.ok)throw new Error('HTTP '+r.status);return r.json();});\n");
        b.append("}\n");
        b.append("function beat(){\n");
        b.append("  fetch('/api/heartbeat',{method:'POST',cache:'no-store',headers:{'Content-Type':'application/json'},\n");
        b.append("    body:JSON.stringify({id:state.id,alias:state.alias,type:state.type})})\n");
        b.append("  .then(function(r){if(!r.ok)throw new Error('bad');state.lastBeatOk=Date.now();setStatus();})\n");
        b.append("  .catch(function(){setStatus();});\n");
        b.append("}\n");
        b.append("var polling=false;\n");
        b.append("function poll(){\n");
        b.append("  if(polling)return;polling=true;\n");
        b.append("  Promise.all([getJson('/api/peers'),getJson('/api/inbox?peerId='+encodeURIComponent(state.id))])\n");
        b.append("  .then(function(r){applyPeers(r[0]);applyInbox(r[1]);})\n");
        b.append("  .catch(function(){})\n");
        b.append("  .then(function(){polling=false;});\n");
        b.append("}\n");
        b.append("\n");
        b.append("/* ---------- radar ---------- */\n");
        b.append("function applyPeers(list){\n");
        b.append("  var others=list.filter(function(p){return p.id!==state.id;})\n");
        b.append("    .sort(function(a,b){return a.alias.localeCompare(b.alias);});\n");
        b.append("  if(state.target!==ALL&&!others.some(function(p){return p.id===state.target;})){\n");
        b.append("    state.target=ALL;toast('Selected device went offline - switched to Broadcast');\n");
        b.append("  }\n");
        b.append("  var sig=others.map(function(p){return p.id+'|'+p.alias+'|'+p.type;}).join(',')+'#'+state.target;\n");
        b.append("  state.peers=others;\n");
        b.append("  if(sig===state.peerSig)return;\n");
        b.append("  state.peerSig=sig;renderRadar();renderDest();\n");
        b.append("}\n");
        b.append("function chip(id,icon,name,sub){\n");
        b.append("  var b=el('button','chip'+(state.target===id?' active':''));\n");
        b.append("  b.type='button';\n");
        b.append("  b.appendChild(el('span','ic',icon));\n");
        b.append("  var d=el('span');d.appendChild(el('b',null,name));d.appendChild(el('small',null,sub));\n");
        b.append("  b.appendChild(d);\n");
        b.append("  b.addEventListener('click',function(){state.target=id;renderRadar();renderDest();});\n");
        b.append("  return b;\n");
        b.append("}\n");
        b.append("function renderRadar(){\n");
        b.append("  var tray=$('#tray'),left=tray.scrollLeft;\n");
        b.append("  tray.textContent='';\n");
        b.append("  tray.appendChild(chip(ALL,cp(0x1F4E1),'Send All','Broadcast'));\n");
        b.append("  state.peers.forEach(function(p){tray.appendChild(chip(p.id,ICON[p.type]||ICON.desktop,p.alias,TYPE_NAME[p.type]||'Device'));});\n");
        b.append("  tray.scrollLeft=left;\n");
        b.append("  $('#rcount').textContent=state.peers.length+' nearby';\n");
        b.append("  $('#rhint').hidden=state.peers.length>0;\n");
        b.append("}\n");
        b.append("function renderDest(){\n");
        b.append("  var d=$('#dest');d.textContent='';\n");
        b.append("  d.appendChild(document.createTextNode('Destination: '));\n");
        b.append("  d.appendChild(el('b',null,destName()));\n");
        b.append("}\n");
        b.append("\n");
        b.append("/* ---------- tabs ---------- */\n");
        b.append("function showTab(name){\n");
        b.append("  state.tab=name;\n");
        b.append("  ['text','files','inbox'].forEach(function(n){\n");
        b.append("    $('#p-'+n).hidden=(n!==name);\n");
        b.append("  });\n");
        b.append("  var tabs=document.querySelectorAll('.tab');\n");
        b.append("  for(var i=0;i<tabs.length;i++)tabs[i].classList.toggle('on',tabs[i].getAttribute('data-tab')===name);\n");
        b.append("  $('#dest').hidden=(name==='inbox');\n");
        b.append("  if(name==='inbox'){state.unread=0;updateBadge();}\n");
        b.append("}\n");
        b.append("function updateBadge(){\n");
        b.append("  var b=$('#nbadge');b.hidden=state.unread<=0;b.textContent=state.unread;\n");
        b.append("}\n");
        b.append("(function(){\n");
        b.append("  var tabs=document.querySelectorAll('.tab');\n");
        b.append("  for(var i=0;i<tabs.length;i++)tabs[i].addEventListener('click',function(){showTab(this.getAttribute('data-tab'));});\n");
        b.append("})();\n");
        b.append("\n");
        b.append("/* ---------- sending ---------- */\n");
        b.append("function sendHeaders(type,target){\n");
        b.append("  return {'X-Target-To':target,'X-Target-From':encodeURIComponent(state.alias),'X-Packet-Type':type,'X-Sender-Id':state.id};\n");
        b.append("}\n");
        b.append("function errText(r){\n");
        b.append("  return r.json().then(function(j){return j.error||('HTTP '+r.status);},function(){return 'HTTP '+r.status;});\n");
        b.append("}\n");
        b.append("function sendText(){\n");
        b.append("  var ta=$('#ta'),t=ta.value;\n");
        b.append("  if(!t.trim()){toast('Type or paste something first');return;}\n");
        b.append("  var target=state.target,name=destName(),btn=$('#sendText');\n");
        b.append("  btn.disabled=true;\n");
        b.append("  var h=sendHeaders('text',target);h['Content-Type']='text/plain;charset=utf-8';\n");
        b.append("  fetch('/api/send',{method:'POST',headers:h,body:t})\n");
        b.append("  .then(function(r){\n");
        b.append("    if(r.ok){ta.value='';updateCount();toast('Sent to '+name);return;}\n");
        b.append("    return errText(r).then(function(m){toast('Failed: '+m);});\n");
        b.append("  })\n");
        b.append("  .catch(function(){toast('Failed: server unreachable');})\n");
        b.append("  .then(function(){btn.disabled=false;});\n");
        b.append("}\n");
        b.append("function updateCount(){\n");
        b.append("  var n=$('#ta').value.length;\n");
        b.append("  $('#charcount').textContent=n?n+' chars':'';\n");
        b.append("}\n");
        b.append("$('#sendText').addEventListener('click',sendText);\n");
        b.append("$('#clearText').addEventListener('click',function(){$('#ta').value='';updateCount();$('#ta').focus();});\n");
        b.append("$('#ta').addEventListener('input',updateCount);\n");
        b.append("$('#ta').addEventListener('keydown',function(e){if(e.key==='Enter'&&(e.ctrlKey||e.metaKey)){e.preventDefault();sendText();}});\n");
        b.append("if(navigator.clipboard&&navigator.clipboard.readText&&window.isSecureContext){\n");
        b.append("  $('#pasteBtn').hidden=false;\n");
        b.append("  $('#pasteBtn').addEventListener('click',function(){\n");
        b.append("    navigator.clipboard.readText().then(function(t){var ta=$('#ta');ta.value+=t;updateCount();},function(){toast('Clipboard access denied');});\n");
        b.append("  });\n");
        b.append("}\n");
        b.append("\n");
        b.append("/* ---------- file upload ---------- */\n");
        b.append("function addUploadRow(file){\n");
        b.append("  var row=el('div','up'),n=el('div','n'),st=el('div','st','Waiting...');\n");
        b.append("  n.appendChild(el('span',null,file.name));n.appendChild(el('span',null,fmtSize(file.size)));\n");
        b.append("  var bar=el('div','bar'),fill=el('i');bar.appendChild(fill);\n");
        b.append("  row.appendChild(n);row.appendChild(bar);row.appendChild(st);\n");
        b.append("  $('#ups').insertBefore(row,$('#ups').firstChild);\n");
        b.append("  return {\n");
        b.append("    progress:function(f){fill.style.width=Math.round(f*100)+'%';st.textContent='Sending... '+Math.round(f*100)+'%';},\n");
        b.append("    ok:function(to){fill.style.width='100%';row.classList.add('ok');st.textContent='Sent to '+to;},\n");
        b.append("    fail:function(m){row.classList.add('bad');fill.style.width='100%';st.textContent='Failed: '+m;}\n");
        b.append("  };\n");
        b.append("}\n");
        b.append("function uploadOne(file,target,targetName){\n");
        b.append("  var ui=addUploadRow(file);\n");
        b.append("  if(file.size>state.maxFile){ui.fail('file is larger than '+fmtSize(state.maxFile));return Promise.resolve();}\n");
        b.append("  return new Promise(function(resolve){\n");
        b.append("    var x=new XMLHttpRequest();\n");
        b.append("    x.open('POST','/api/send');\n");
        b.append("    var h=sendHeaders('file',target);\n");
        b.append("    for(var k in h)x.setRequestHeader(k,h[k]);\n");
        b.append("    x.setRequestHeader('X-File-Name',encodeURIComponent(file.name));\n");
        b.append("    x.setRequestHeader('Content-Type','application/octet-stream');\n");
        b.append("    x.upload.onprogress=function(e){if(e.lengthComputable)ui.progress(e.loaded/e.total);};\n");
        b.append("    x.onload=function(){\n");
        b.append("      if(x.status>=200&&x.status<300)ui.ok(targetName);\n");
        b.append("      else{var m='HTTP '+x.status;try{m=JSON.parse(x.responseText).error||m;}catch(e){}ui.fail(m);}\n");
        b.append("      resolve();\n");
        b.append("    };\n");
        b.append("    x.onerror=function(){ui.fail('connection lost');resolve();};\n");
        b.append("    x.send(file);\n");
        b.append("  });\n");
        b.append("}\n");
        b.append("function handleFiles(list){\n");
        b.append("  if(!list||!list.length)return;\n");
        b.append("  showTab('files');\n");
        b.append("  var target=state.target,name=destName();\n");
        b.append("  for(var i=0;i<list.length;i++){\n");
        b.append("    (function(f){state.chain=state.chain.then(function(){return uploadOne(f,target,name);});})(list[i]);\n");
        b.append("  }\n");
        b.append("}\n");
        b.append("$('#fileInput').addEventListener('change',function(){handleFiles(this.files);this.value='';});\n");
        b.append("(function(){\n");
        b.append("  var dz=$('#dz');\n");
        b.append("  window.addEventListener('dragover',function(e){e.preventDefault();dz.classList.add('over');});\n");
        b.append("  window.addEventListener('dragleave',function(e){if(!e.relatedTarget)dz.classList.remove('over');});\n");
        b.append("  window.addEventListener('drop',function(e){\n");
        b.append("    e.preventDefault();dz.classList.remove('over');\n");
        b.append("    if(e.dataTransfer&&e.dataTransfer.files&&e.dataTransfer.files.length)handleFiles(e.dataTransfer.files);\n");
        b.append("  });\n");
        b.append("  window.addEventListener('paste',function(e){\n");
        b.append("    var fl=e.clipboardData&&e.clipboardData.files;\n");
        b.append("    if(fl&&fl.length){e.preventDefault();handleFiles(fl);}\n");
        b.append("  });\n");
        b.append("})();\n");
        b.append("\n");
        b.append("/* ---------- inbox ---------- */\n");
        b.append("function clearedKey(){return 'cf_cleared_'+state.id;}\n");
        b.append("function applyInbox(list){\n");
        b.append("  list.forEach(function(p){if(p.ts>state.maxTs)state.maxTs=p.ts;});\n");
        b.append("  list=list.filter(function(p){return p.ts>state.cleared;});\n");
        b.append("  var fresh=[];\n");
        b.append("  list.forEach(function(p){\n");
        b.append("    if(!state.seen[p.id]){state.seen[p.id]=1;if(!state.firstLoad)fresh.push(p);}\n");
        b.append("  });\n");
        b.append("  state.firstLoad=false;\n");
        b.append("  if(fresh.length){\n");
        b.append("    if(state.tab!=='inbox'){state.unread+=fresh.length;updateBadge();}\n");
        b.append("    var last=fresh[fresh.length-1];\n");
        b.append("    toast('New '+(last.type==='file'?'file':'text')+' from '+last.from);\n");
        b.append("    try{if(navigator.vibrate)navigator.vibrate(60);}catch(e){}\n");
        b.append("  }\n");
        b.append("  var sig=list.map(function(p){return p.id;}).join(',');\n");
        b.append("  if(sig===state.feedSig)return;\n");
        b.append("  state.feedSig=sig;renderFeed(list);\n");
        b.append("}\n");
        b.append("function renderFeed(list){\n");
        b.append("  var feed=$('#feed');feed.textContent='';\n");
        b.append("  if(!list.length){\n");
        b.append("    var e=el('div','card empty');\n");
        b.append("    e.appendChild(el('big',null,cp(0x1F4ED)));\n");
        b.append("    e.appendChild(document.createTextNode('Nothing received yet. Items sent to this device appear here.'));\n");
        b.append("    feed.appendChild(e);return;\n");
        b.append("  }\n");
        b.append("  list.slice().sort(function(a,b){return b.ts-a.ts;}).forEach(function(p){feed.appendChild(card(p));});\n");
        b.append("}\n");
        b.append("function card(p){\n");
        b.append("  var c=el('div','card item'),head=el('div','ihead');\n");
        b.append("  head.appendChild(el('span','from',p.from));\n");
        b.append("  head.appendChild(el('span','badge '+(p.mode==='direct'?'direct':'bcast'),p.mode==='direct'?'Direct':'Broadcast'));\n");
        b.append("  head.appendChild(el('span','time',fmtTime(p.ts)));\n");
        b.append("  c.appendChild(head);\n");
        b.append("  if(p.type==='text'){\n");
        b.append("    c.appendChild(el('pre','txt',p.text));\n");
        b.append("    var row=el('div','row');row.style.marginTop='0';\n");
        b.append("    var cb=el('button','btn primary small','Copy');cb.type='button';\n");
        b.append("    cb.addEventListener('click',function(){\n");
        b.append("      copyText(p.text).then(function(){cb.textContent='Copied';toast('Copied to clipboard');setTimeout(function(){cb.textContent='Copy';},1500);},\n");
        b.append("        function(){toast('Copy blocked - long-press the text to copy');});\n");
        b.append("    });\n");
        b.append("    row.appendChild(cb);\n");
        b.append("    if(isHttpUrl(p.text)){\n");
        b.append("      var a=el('a','btn small','Open link');a.href=p.text.trim();a.target='_blank';a.rel='noopener noreferrer';row.appendChild(a);\n");
        b.append("    }\n");
        b.append("    c.appendChild(row);\n");
        b.append("  }else{\n");
        b.append("    var enc=encodeURIComponent(p.fileName);\n");
        b.append("    if(isImage(p.fileName)){\n");
        b.append("      var link=document.createElement('a');link.href='/api/download?file='+enc+'&inline=1';link.target='_blank';link.rel='noopener';\n");
        b.append("      var img=document.createElement('img');img.className='thumb';img.loading='lazy';img.alt=p.fileName;img.src='/api/download?file='+enc+'&inline=1';\n");
        b.append("      link.appendChild(img);c.appendChild(link);\n");
        b.append("    }\n");
        b.append("    var f=el('div','file');\n");
        b.append("    f.appendChild(el('div','fi',fileIcon(p.fileName)));\n");
        b.append("    var fn=el('div','fn');fn.appendChild(el('b',null,p.fileName));fn.appendChild(el('span','sub',fmtSize(p.size)));\n");
        b.append("    f.appendChild(fn);\n");
        b.append("    var dl=el('a','btn primary small','Download');dl.href='/api/download?file='+enc;dl.setAttribute('download',p.fileName);\n");
        b.append("    f.appendChild(dl);c.appendChild(f);\n");
        b.append("  }\n");
        b.append("  return c;\n");
        b.append("}\n");
        b.append("$('#clearFeed').addEventListener('click',function(){\n");
        b.append("  state.cleared=state.maxTs;\n");
        b.append("  lsSet(clearedKey(),String(state.cleared));\n");
        b.append("  state.feedSig='';renderFeed([]);\n");
        b.append("  toast('Feed cleared');\n");
        b.append("});\n");
        b.append("\n");
        b.append("/* ---------- pairing modal ---------- */\n");
        b.append("function openModal(){\n");
        b.append("  $('#qrimg').src='/api/qr?'+Date.now();\n");
        b.append("  var u=$('#urls');u.textContent='';\n");
        b.append("  (state.urls.length?state.urls:[location.origin]).forEach(function(x){u.appendChild(el('div','u',x));});\n");
        b.append("  $('#modal').hidden=false;\n");
        b.append("}\n");
        b.append("$('#pairBtn').addEventListener('click',openModal);\n");
        b.append("$('#mclose').addEventListener('click',function(){$('#modal').hidden=true;});\n");
        b.append("$('#modal').addEventListener('click',function(e){if(e.target===this)this.hidden=true;});\n");
        b.append("\n");
        b.append("/* ---------- boot ---------- */\n");
        b.append("initIdentity(function(){\n");
        b.append("  var c=Number(lsGet(clearedKey()));state.cleared=isNaN(c)?0:c;\n");
        b.append("  renderHeader();renderRadar();renderDest();showTab('text');setStatus();\n");
        b.append("  getJson('/api/info').then(function(i){state.urls=i.urls||[];if(i.maxFileBytes)state.maxFile=i.maxFileBytes;}).catch(function(){});\n");
        b.append("  beat();poll();\n");
        b.append("  setInterval(beat,2500);\n");
        b.append("  setInterval(poll,2000);\n");
        b.append("  setInterval(setStatus,1000);\n");
        b.append("  document.addEventListener('visibilitychange',function(){if(!document.hidden){beat();poll();}});\n");
        b.append("});\n");
        b.append("})();\n");
        b.append("</script>\n");
        b.append("</body>\n");
        b.append("</html>\n");
        return b.toString();
    }


    // ================================================================ QR code
    /** Compact QR encoder: byte mode, ECC level M, versions 1-10 (up to ~213 bytes). */
    static final class QrCode {
        static final int[] ECC_PER_BLOCK = {-1, 10, 16, 26, 18, 24, 16, 18, 22, 22, 26};
        static final int[] NUM_BLOCKS = {-1, 1, 1, 1, 2, 2, 4, 4, 4, 5, 5};

        final int version, size;
        private final boolean[][] modules;
        private final boolean[][] isFunction;

        static QrCode encode(String text) {
            byte[] data = text.getBytes(StandardCharsets.UTF_8);
            int ver = 1;
            for (;; ver++) {
                if (ver > 10) throw new IllegalArgumentException("text too long for QR");
                int ccBits = ver <= 9 ? 8 : 16;
                if (4 + ccBits + data.length * 8 <= dataCodewords(ver) * 8) break;
            }
            int ccBits = ver <= 9 ? 8 : 16;
            List<Boolean> bits = new ArrayList<>();
            appendBits(bits, 4, 4);                    // byte mode indicator 0100
            appendBits(bits, data.length, ccBits);
            for (byte b : data) appendBits(bits, b & 0xFF, 8);
            int capBits = dataCodewords(ver) * 8;
            appendBits(bits, 0, Math.min(4, capBits - bits.size()));
            appendBits(bits, 0, (8 - bits.size() % 8) % 8);
            for (int pad = 0xEC; bits.size() < capBits; pad ^= 0xEC ^ 0x11) appendBits(bits, pad, 8);
            byte[] cw = new byte[bits.size() / 8];
            for (int i = 0; i < bits.size(); i++) if (bits.get(i)) cw[i >>> 3] |= (byte) (1 << (7 - (i & 7)));
            return new QrCode(ver, cw);
        }

        private static void appendBits(List<Boolean> bits, int val, int len) {
            for (int i = len - 1; i >= 0; i--) bits.add(((val >>> i) & 1) != 0);
        }

        private static int rawModules(int ver) {
            int result = (16 * ver + 128) * ver + 64;
            if (ver >= 2) {
                int numAlign = ver / 7 + 2;
                result -= (25 * numAlign - 10) * numAlign - 55;
                if (ver >= 7) result -= 36;
            }
            return result;
        }

        private static int dataCodewords(int ver) {
            return rawModules(ver) / 8 - ECC_PER_BLOCK[ver] * NUM_BLOCKS[ver];
        }

        private QrCode(int ver, byte[] dataCodewords) {
            version = ver;
            size = ver * 4 + 17;
            modules = new boolean[size][size];
            isFunction = new boolean[size][size];
            drawFunctionPatterns();
            drawCodewords(addEccAndInterleave(dataCodewords));
            int best = 0, bestPenalty = Integer.MAX_VALUE;
            for (int m = 0; m < 8; m++) {
                applyMask(m);
                drawFormatBits(m);
                int p = penalty();
                if (p < bestPenalty) { bestPenalty = p; best = m; }
                applyMask(m);                          // undo (XOR)
            }
            applyMask(best);
            drawFormatBits(best);
        }

        boolean isDark(int x, int y) {
            return x >= 0 && x < size && y >= 0 && y < size && modules[y][x];
        }

        String toSvg(int border) {
            int dim = size + border * 2;
            StringBuilder sb = new StringBuilder();
            sb.append("<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 ").append(dim).append(' ').append(dim)
              .append("\" shape-rendering=\"crispEdges\"><rect width=\"100%\" height=\"100%\" fill=\"#ffffff\"/><path d=\"");
            for (int y = 0; y < size; y++)
                for (int x = 0; x < size; x++)
                    if (modules[y][x]) sb.append('M').append(x + border).append(',').append(y + border).append("h1v1h-1z");
            return sb.append("\" fill=\"#000000\"/></svg>").toString();
        }

        // ---- function patterns
        private void setFn(int x, int y, boolean dark) {
            modules[y][x] = dark;
            isFunction[y][x] = true;
        }

        private void drawFunctionPatterns() {
            for (int i = 0; i < size; i++) { setFn(6, i, i % 2 == 0); setFn(i, 6, i % 2 == 0); }
            drawFinder(3, 3);
            drawFinder(size - 4, 3);
            drawFinder(3, size - 4);
            int[] pos = alignmentPositions();
            int n = pos.length;
            for (int i = 0; i < n; i++)
                for (int j = 0; j < n; j++)
                    if (!((i == 0 && j == 0) || (i == 0 && j == n - 1) || (i == n - 1 && j == 0)))
                        drawAlignment(pos[i], pos[j]);
            drawFormatBits(0);
            drawVersion();
        }

        private int[] alignmentPositions() {
            if (version == 1) return new int[0];
            int numAlign = version / 7 + 2;
            int step = (version * 8 + numAlign * 3 + 5) / (numAlign * 4 - 4) * 2;
            int[] r = new int[numAlign];
            r[0] = 6;
            for (int i = numAlign - 1, p = size - 7; i >= 1; i--, p -= step) r[i] = p;
            return r;
        }

        private void drawFinder(int cx, int cy) {
            for (int dy = -4; dy <= 4; dy++)
                for (int dx = -4; dx <= 4; dx++) {
                    int dist = Math.max(Math.abs(dx), Math.abs(dy));
                    int xx = cx + dx, yy = cy + dy;
                    if (xx >= 0 && xx < size && yy >= 0 && yy < size) setFn(xx, yy, dist != 2 && dist != 4);
                }
        }

        private void drawAlignment(int cx, int cy) {
            for (int dy = -2; dy <= 2; dy++)
                for (int dx = -2; dx <= 2; dx++)
                    setFn(cx + dx, cy + dy, Math.max(Math.abs(dx), Math.abs(dy)) != 1);
        }

        private static boolean bit(int x, int i) { return ((x >>> i) & 1) != 0; }

        private void drawFormatBits(int mask) {
            int data = (0 << 3) | mask;                // ECC level M = 00
            int rem = data;
            for (int i = 0; i < 10; i++) rem = (rem << 1) ^ ((rem >>> 9) * 0x537);
            int bits = ((data << 10) | rem) ^ 0x5412;
            for (int i = 0; i <= 5; i++) setFn(8, i, bit(bits, i));
            setFn(8, 7, bit(bits, 6));
            setFn(8, 8, bit(bits, 7));
            setFn(7, 8, bit(bits, 8));
            for (int i = 9; i < 15; i++) setFn(14 - i, 8, bit(bits, i));
            for (int i = 0; i < 8; i++) setFn(size - 1 - i, 8, bit(bits, i));
            for (int i = 8; i < 15; i++) setFn(8, size - 15 + i, bit(bits, i));
            setFn(8, size - 8, true);
        }

        private void drawVersion() {
            if (version < 7) return;
            int rem = version;
            for (int i = 0; i < 12; i++) rem = (rem << 1) ^ ((rem >>> 11) * 0x1F25);
            int bits = (version << 12) | rem;
            for (int i = 0; i < 18; i++) {
                boolean c = bit(bits, i);
                int a = size - 11 + i % 3, b = i / 3;
                setFn(a, b, c);
                setFn(b, a, c);
            }
        }

        // ---- error correction
        private byte[] addEccAndInterleave(byte[] data) {
            int numBlocks = NUM_BLOCKS[version], eccLen = ECC_PER_BLOCK[version];
            int raw = rawModules(version) / 8;
            int numShort = numBlocks - raw % numBlocks;
            int shortLen = raw / numBlocks;
            byte[][] blocks = new byte[numBlocks][];
            byte[] div = rsDivisor(eccLen);
            for (int i = 0, k = 0; i < numBlocks; i++) {
                byte[] dat = Arrays.copyOfRange(data, k, k + shortLen - eccLen + (i < numShort ? 0 : 1));
                k += dat.length;
                byte[] ecc = rsRemainder(dat, div);
                if (i < numShort) dat = Arrays.copyOf(dat, dat.length + 1);
                byte[] block = Arrays.copyOf(dat, shortLen + 1);
                System.arraycopy(ecc, 0, block, block.length - eccLen, ecc.length);
                blocks[i] = block;
            }
            byte[] result = new byte[raw];
            for (int i = 0, k = 0; i < blocks[0].length; i++)
                for (int j = 0; j < blocks.length; j++)
                    if (i != shortLen - eccLen || j >= numShort) result[k++] = blocks[j][i];
            return result;
        }

        private static byte[] rsDivisor(int degree) {
            byte[] r = new byte[degree];
            r[degree - 1] = 1;
            int root = 1;
            for (int i = 0; i < degree; i++) {
                for (int j = 0; j < degree; j++) {
                    r[j] = (byte) gfMul(r[j] & 0xFF, root);
                    if (j + 1 < degree) r[j] ^= r[j + 1];
                }
                root = gfMul(root, 0x02);
            }
            return r;
        }

        private static byte[] rsRemainder(byte[] data, byte[] div) {
            byte[] r = new byte[div.length];
            for (byte b : data) {
                int factor = (b ^ r[0]) & 0xFF;
                System.arraycopy(r, 1, r, 0, r.length - 1);
                r[r.length - 1] = 0;
                for (int i = 0; i < r.length; i++) r[i] ^= (byte) gfMul(div[i] & 0xFF, factor);
            }
            return r;
        }

        private static int gfMul(int x, int y) {
            int z = 0;
            for (int i = 7; i >= 0; i--) {
                z = (z << 1) ^ ((z >>> 7) * 0x11D);
                z ^= ((y >>> i) & 1) * x;
            }
            return z & 0xFF;
        }

        // ---- placement, masking, scoring
        private void drawCodewords(byte[] data) {
            int i = 0;
            for (int right = size - 1; right >= 1; right -= 2) {
                if (right == 6) right = 5;
                for (int vert = 0; vert < size; vert++) {
                    for (int j = 0; j < 2; j++) {
                        int x = right - j;
                        boolean upward = ((right + 1) & 2) == 0;
                        int y = upward ? size - 1 - vert : vert;
                        if (!isFunction[y][x] && i < data.length * 8) {
                            modules[y][x] = bit(data[i >>> 3] & 0xFF, 7 - (i & 7));
                            i++;
                        }
                    }
                }
            }
        }

        private void applyMask(int mask) {
            for (int y = 0; y < size; y++)
                for (int x = 0; x < size; x++) {
                    boolean invert;
                    switch (mask) {
                        case 0: invert = (x + y) % 2 == 0; break;
                        case 1: invert = y % 2 == 0; break;
                        case 2: invert = x % 3 == 0; break;
                        case 3: invert = (x + y) % 3 == 0; break;
                        case 4: invert = (x / 3 + y / 2) % 2 == 0; break;
                        case 5: invert = x * y % 2 + x * y % 3 == 0; break;
                        case 6: invert = (x * y % 2 + x * y % 3) % 2 == 0; break;
                        default: invert = ((x + y) % 2 + x * y % 3) % 2 == 0; break;
                    }
                    if (invert && !isFunction[y][x]) modules[y][x] ^= true;
                }
        }

        private int penalty() {
            int result = 0;
            for (int pass = 0; pass < 2; pass++) {                 // rows, then columns
                for (int a = 0; a < size; a++) {
                    int run = 1;
                    for (int b = 1; b < size; b++) {
                        if (get(pass, a, b) == get(pass, a, b - 1)) run++;
                        else { if (run >= 5) result += run - 2; run = 1; }
                    }
                    if (run >= 5) result += run - 2;
                    for (int b = 0; b + 11 <= size; b++) {
                        if (matches(pass, a, b, new int[]{1, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0})) result += 40;
                        if (matches(pass, a, b, new int[]{0, 0, 0, 0, 1, 0, 1, 1, 1, 0, 1})) result += 40;
                    }
                }
            }
            int dark = 0;
            for (int y = 0; y < size; y++)
                for (int x = 0; x < size; x++) {
                    if (modules[y][x]) dark++;
                    if (x + 1 < size && y + 1 < size) {
                        boolean c = modules[y][x];
                        if (c == modules[y][x + 1] && c == modules[y + 1][x] && c == modules[y + 1][x + 1]) result += 3;
                    }
                }
            int total = size * size;
            int k = (Math.abs(dark * 20 - total * 10) + total - 1) / total - 1;
            return result + k * 10;
        }

        private boolean get(int pass, int a, int b) { return pass == 0 ? modules[a][b] : modules[b][a]; }

        private boolean matches(int pass, int a, int b, int[] pat) {
            for (int i = 0; i < pat.length; i++) if (get(pass, a, b + i) != (pat[i] == 1)) return false;
            return true;
        }
    }
}
