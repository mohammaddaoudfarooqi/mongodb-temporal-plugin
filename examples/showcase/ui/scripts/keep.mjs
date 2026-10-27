// The build empties ../web/dist; restore the placeholder Git tracks so
// the Go package embeds something before the UI is built.
import { writeFileSync } from "node:fs";
writeFileSync(new URL("../../web/dist/.gitkeep", import.meta.url), "");
