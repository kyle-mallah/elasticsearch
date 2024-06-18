/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

/**
 * Computes the minimum bounding box (envelope) for the supplied geometry.
 * The function `st_envelope` is defined in the <a href="https://postgis.net/docs/ST_Envelope.html">PostGIS:ST_Envelope</a> documentation.
 */
public class StEnvelope extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY
        = new NamedWriteableRegistry.Entry(Expression.class, "StEnvelope", StEnvelope::new);

    @FunctionInfo(
        returnType = "geo_shape",
        description = "Computes the minimum bounding box (envelope) for the supplied geometry.",
        examples = @Example(file = "spatial", tag = "st_envelope")
    )
    public StEnvelope(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_shape", "cartesian_shape" },
            description = "Expression of type `geo_shape` or `cartesian_shape`. If `null`, the function returns `null`."
        ) Expression field
    ) {
        super(source, field);
    }

    private StEnvelope(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isSpatial(field(), sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        return new StEnvelopeFromWKBEvaluator.Factory(toEvaluator.apply(field()), source());
    }

    @Override
    public DataType dataType() {
        return GEO_SHAPE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StEnvelope(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StEnvelope::new, field());
    }

    @ConvertEvaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromWellKnownBinary(BytesRef in) {
        Geometry geometry = GeometryUtils.fromWellKnownBinary(in);
        if (geometry == null) {
            throw new IllegalArgumentException("Invalid WKB geometry");
        }
        // TODO refactor / cleanup duplciation
        Rectangle envelope = geometry.visit(new GeometryVisitor<>() {
            @Override
            public Rectangle visit(Circle circle) {
                double minX = circle.getX() - circle.getRadiusMeters(); // TODO which units should this be?
                double maxX = circle.getX() + circle.getRadiusMeters();
                double minY = circle.getY() - circle.getRadiusMeters();
                double maxY = circle.getY() + circle.getRadiusMeters();
                return new Rectangle(minX, maxX, minY, maxY);
            }

            @Override
            public Rectangle visit(GeometryCollection<?> collection) {
                double minX = Double.POSITIVE_INFINITY;
                double maxX = Double.NEGATIVE_INFINITY;
                double minY = Double.POSITIVE_INFINITY;
                double maxY = Double.NEGATIVE_INFINITY;
                for (Geometry g : collection) {
                    Rectangle r = g.visit(this);
                    minX = Math.min(minX, r.getMinX());
                    maxX = Math.max(maxX, r.getMaxX());
                    minY = Math.min(minY, r.getMinY());
                    maxY = Math.max(maxY, r.getMaxY());
                }
                return new Rectangle(minX, maxX, minY, maxY);
            }

            @Override
            public Rectangle visit(Line line) {
                double minX = Double.POSITIVE_INFINITY;
                double maxX = Double.NEGATIVE_INFINITY;
                double minY = Double.POSITIVE_INFINITY;
                double maxY = Double.NEGATIVE_INFINITY;
                for (int i = 0; i < line.length(); i++) {
                    double x = line.getX(i);
                    double y = line.getY(i);
                    minX = Math.min(minX, x);
                    maxX = Math.max(maxX, x);
                    minY = Math.min(minY, y);
                    maxY = Math.max(maxY, y);
                }
                return new Rectangle(minX, maxX, minY, maxY);
            }

            @Override
            public Rectangle visit(LinearRing ring) {
                return visit((Line) ring);
            }

            @Override
            public Rectangle visit(MultiLine multiLine) {
                double minX = Double.POSITIVE_INFINITY;
                double maxX = Double.NEGATIVE_INFINITY;
                double minY = Double.POSITIVE_INFINITY;
                double maxY = Double.NEGATIVE_INFINITY;
                for (Line line : multiLine) {
                    Rectangle r = line.visit(this);
                    minX = Math.min(minX, r.getMinX());
                    maxX = Math.max(maxX, r.getMaxX());
                    minY = Math.min(minY, r.getMinY());
                    maxY = Math.max(maxY, r.getMaxY());
                }
                return new Rectangle(minX, maxX, minY, maxY);
            }

            @Override
            public Rectangle visit(MultiPoint multiPoint) {
                double minX = Double.POSITIVE_INFINITY;
                double maxX = Double.NEGATIVE_INFINITY;
                double minY = Double.POSITIVE_INFINITY;
                double maxY = Double.NEGATIVE_INFINITY;
                for (Point point : multiPoint) {
                    double x = point.getX();
                    double y = point.getY();
                    minX = Math.min(minX, x);
                    maxX = Math.max(maxX, x);
                    minY = Math.min(minY, y);
                    maxY = Math.max(maxY, y);
                }
                return new Rectangle(minX, maxX, minY, maxY);
            }

            @Override
            public Rectangle visit(MultiPolygon multiPolygon) {
                double minX = Double.POSITIVE_INFINITY;
                double maxX = Double.NEGATIVE_INFINITY;
                double minY = Double.POSITIVE_INFINITY;
                double maxY = Double.NEGATIVE_INFINITY;
                for (Polygon polygon : multiPolygon) {
                    Rectangle r = polygon.visit(this);
                    minX = Math.min(minX, r.getMinX());
                    maxX = Math.max(maxX, r.getMaxX());
                    minY = Math.min(minY, r.getMinY());
                    maxY = Math.max(maxY, r.getMaxY());
                }
                return new Rectangle(minX, maxX, minY, maxY);
            }

            @Override
            public Rectangle visit(Point point) {
                return new Rectangle(point.getX(), point.getX(), point.getY(), point.getY());
            }

            @Override
            public Rectangle visit(Polygon polygon) {
                double minX = Double.POSITIVE_INFINITY;
                double maxX = Double.NEGATIVE_INFINITY;
                double minY = Double.POSITIVE_INFINITY;
                double maxY = Double.NEGATIVE_INFINITY;
                // TODO
                //  Iterate through interior rings to find bounds
                //  Then apply exterior ring to bounds to get final result
                return new Rectangle(minX, maxX, minY, maxY);
            }

            @Override
            public Rectangle visit(Rectangle rectangle) {
                return rectangle;
            }
        });
        LinearRing linearRing = new LinearRing(
            new double[]{envelope.getMinX(), envelope.getMinX(), envelope.getMaxX(), envelope.getMaxX(), envelope.getMinX()},
            new double[]{envelope.getMinY(), envelope.getMaxY(), envelope.getMaxY(), envelope.getMinY(), envelope.getMinY()}
        );

        byte[] wkb = WellKnownBinary.toWKB(new Polygon(linearRing), ByteOrder.BIG_ENDIAN);
        return new BytesRef(wkb);
    }

}
