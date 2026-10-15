package com.authzed.examples.materialize;

import com.authzed.api.materialize.v0.WatchPermissionSetsResponse;

/** Carries the original Materialize control response so the caller can rebuild its snapshot. */
public final class RebuildRequiredException extends Exception {
    private static final long serialVersionUID = 1L;
    private final WatchPermissionSetsResponse control;

    public RebuildRequiredException(WatchPermissionSetsResponse control) {
        super("Materialize sent " + control.getResponseCase()
                + "; hand control to the snapshot/rebuild state machine");
        this.control = control;
    }

    public WatchPermissionSetsResponse control() { return control; }
}
