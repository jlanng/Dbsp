/* Loaded before the admin UI's scripts by the tables check's copy of index.html. The live
   feed is an EventSource that never ends, and a request that never ends stops the
   headless browser's virtual clock for good; without the feed the UI loads each page by
   itself, which is what the check needs. */
window.EventSource = class { constructor() { this.readyState = 0; } addEventListener() {} close() {} static get CLOSED() { return 2; } };
