/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.geometry.Geometry;

public class GeometryUtils {

    private static final GeometryParser geometryParser = new GeometryParser(true, true, true);

    // TODO there must be standard way already to map BytesRef to a Geometry
    public static Geometry fromWellKnownBinary(BytesRef wkb) {
        return geometryParser.parseGeometry(wkb);
    }
}
