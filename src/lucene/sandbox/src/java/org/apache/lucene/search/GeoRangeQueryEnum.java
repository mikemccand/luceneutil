package org.apache.lucene.search;

import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.apache.lucene.spatial.util.GeoUtils;
import org.apache.lucene.util.SloppyMath;

/**
 * Simple enumerator for testing BBox, Distance, and Polygon queries
 */
abstract class GeoRangeQueryEnum {
    public static class BBox extends GeoRangeQueryEnum {
        @Override
        protected boolean cellIntersectsShape(final double minLon, final double minLat, final double maxLon, final double maxLat) {
            return cellIntersectsMBR(minLon, minLat, maxLon, maxLat);
        }

        @Override
        protected boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat) {
            return GeoRelationUtils.rectCrosses(minLon, minLat, maxLon, maxLat, this.mbr.minLon, this.mbr.minLat, this.mbr.maxLon, this.mbr.maxLat);
        }

        @Override
        protected boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
            return GeoRelationUtils.rectWithin(minLon, minLat, maxLon, maxLat, this.mbr.minLon, this.mbr.minLat, this.mbr.maxLon, this.mbr.maxLat);
        }

        public BBox(double minLon, double minLat, double maxLon, double maxLat) {
            super(minLon, minLat, maxLon, maxLat);
        }
    }

    public static class Distance extends GeoRangeQueryEnum {
        @Override
        protected boolean cellIntersectsShape(final double minLon, final double minLat, final double maxLon, final double maxLat) {
            return (cellContains(minLon, minLat, maxLon, maxLat)
                    || cellWithin(minLon, minLat, maxLon, maxLat) || cellCrosses(minLon, minLat, maxLon, maxLat));
        }

        @Override
        protected boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat) {
            return GeoRelationUtils.rectCrossesCircle(minLon, minLat, maxLon, maxLat, cntrLon, cntrLat, radius, true);
        }

        @Override
        protected boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
            return GeoRelationUtils.rectWithinCircle(minLon, minLat, maxLon, maxLat, cntrLon, cntrLat, radius, true);
        }

        @Override
        protected short computeMaxShift() {
            return (short)(GeoPointField.PRECISION_STEP * ((short)((radius>1000000) ? 5 : 4)));
        }

        public Distance(double cntrLon, double cntrLat, double radius) {
            this(GeoUtils.circleToBBox(cntrLon, cntrLat, radius), cntrLon, cntrLat, radius);
        }

        public Distance(GeoRect bbox, double cntrLon, double cntrLat, double radius) {
            super(bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat);
            this.cntrLon = cntrLon;
            this.cntrLat = cntrLat;
            this.radius = radius;
        }

        public double cntrLon;
        public double cntrLat;
        public double radius;
    }

    public static class Polygon extends GeoRangeQueryEnum {
        @Override
        protected boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat) {
            return GeoRelationUtils.rectCrossesPolyApprox(minLon, minLat, maxLon, maxLat, x, y, this.mbr.minLon, this.mbr.minLat, this.mbr.maxLon, this.mbr.maxLat);
        }

        @Override
        protected boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat) {
            return GeoRelationUtils.rectWithinPolyApprox(minLon, minLat, maxLon, maxLat, x, y, this.mbr.minLon, this.mbr.minLat, this.mbr.maxLon, this.mbr.maxLat);
        }

        @Override
        protected boolean cellIntersectsShape(final double minLon, final double minLat, final double maxLon, final double maxLat) {
            return cellContains(minLon, minLat, maxLon, maxLat) || cellWithin(minLon, minLat, maxLon, maxLat)
                    || cellCrosses(minLon, minLat, maxLon, maxLat);        }

        public Polygon(double[] x, double[] y) {
            this(GeoUtils.polyToBBox(x, y), x, y);
        }

        Polygon(GeoRect bbox, double[] x, double[] y) {
            super(bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat);
            this.x = x;
            this.y = y;
        }

        public double[] x;
        public double[] y;
    }

    protected GeoRangeQueryEnum(double minLon, double minLat, double maxLon, double maxLat) {
        this.mbr = new GeoRect(minLon, maxLon, minLat, maxLat);
    }

    protected boolean cellIntersectsMBR(final double minLon, final double minLat, final double maxLon, final double maxLat) {
        return GeoRelationUtils.rectIntersects(minLon, minLat, maxLon, maxLat, this.mbr.minLon, this.mbr.minLat, this.mbr.maxLon, this.mbr.maxLat);
    }

    protected boolean cellContains(final double minLon, final double minLat, final double maxLon, final double maxLat) {
        return GeoRelationUtils.rectWithin(this.mbr.minLon, this.mbr.minLat, this.mbr.maxLon, this.mbr.maxLat, minLon, minLat, maxLon, maxLat);
    }

    abstract boolean cellIntersectsShape(final double minLon, final double minLat, final double maxLon, final double maxLat);
    abstract boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat);
    abstract boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat);

    protected short computeMaxShift() {
        double midLon = (this.mbr.minLon + this.mbr.maxLon) * 0.5;
        double midLat = (this.mbr.minLat + this.mbr.maxLat) * 0.5;
        return (short) (GeoPointField.PRECISION_STEP * ((SloppyMath.haversin(this.mbr.minLat, this.mbr.minLon, midLat, midLon)*1000 >
                1000000) ? 5 : 4));
    }

    protected GeoRect mbr;
}
