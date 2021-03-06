<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <!-- **********************************************************************
         Sync Repository Import

         CSV fields:

         UUID............................sync_repository.uuid
         Name............................sync_repository.name
         Type............................sync_repository.apitype
                                         eden|filesync
         Data Type.......................sync_repository.backend (for filesync)
                                         eden
         URL.............................sync_repository.url
         Path............................sync_repository.path (for filesync)
         Synchronize UUIDs...............sync_repository.synchronise_uuids
                                         true|false

    *********************************************************************** -->
    <xsl:output method="xml"/>

    <xsl:include href="../../xml/commons.xsl"/>

    <!-- ****************************************************************** -->

    <xsl:template match="/">
        <s3xml>
            <xsl:apply-templates select="table/row"/>
        </s3xml>
    </xsl:template>

    <!-- ****************************************************************** -->
    <xsl:template match="row">

        <xsl:variable name="Name" select="col[@field='Name']/text()"/>
        <xsl:variable name="Type" select="col[@field='Type']/text()"/>

        <xsl:if test="$Name!='' and $Type!=''">

            <resource name="sync_repository">

                <!-- Repository UUID -->
                <xsl:variable name="UUID" select="col[@field='UUID']/text()"/>
                <xsl:if test="$UUID!=''">
                    <xsl:attribute name="uuid">
                        <xsl:value-of select="col[@field='UUID']"/>
                    </xsl:attribute>
                </xsl:if>

                <!-- Name and API type -->
                <data field="name">
                    <xsl:value-of select="$Name"/>
                </data>
                <data field="apitype">
                    <xsl:value-of select="$Type"/>
                </data>

                <!-- URL or File Path -->
                <xsl:variable name="URL" select="col[@field='URL']/text()"/>
                <xsl:variable name="Path" select="col[@field='Path']/text()"/>
                <xsl:choose>
                    <xsl:when test="$Type='filesync'">
                        <xsl:if test="$Path!=''">
                            <data field="path">
                                <xsl:value-of select="$Path"/>
                            </data>
                        </xsl:if>
                        <xsl:variable name="Backend" select="col[@field='Data Type']/text()"/>
                        <xsl:if test="$Backend!=''">
                            <data field="backend">
                                <xsl:value-of select="$Backend"/>
                            </data>
                        </xsl:if>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:if test="$URL!=''">
                            <data field="url">
                                <xsl:value-of select="$URL"/>
                            </data>
                        </xsl:if>
                    </xsl:otherwise>
                </xsl:choose>

                <!-- Synchronize UUIDs? -->
                <xsl:variable name="SynchroniseUUIDs" select="col[@field='Synchronize UUIDs']/text()"/>
                <data field="synchronise_uuids">
                    <xsl:attribute name="value">
                        <xsl:choose>
                            <xsl:when test="$SynchroniseUUIDs='true'">
                                <xsl:value-of select="'true'"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="'false'"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:attribute>
                </data>

                <!-- Data Sets -->
                <xsl:variable name="Datasets" select="col[@field='Data Sets']/text()"/>
                <xsl:if test="$Datasets!=''">
                    <xsl:call-template name="splitList">
                        <xsl:with-param name="list"><xsl:value-of select="$Datasets"/></xsl:with-param>
                        <xsl:with-param name="arg">datasets</xsl:with-param>
                    </xsl:call-template>
                </xsl:if>

            </resource>
        </xsl:if>

    </xsl:template>

    <!-- ****************************************************************** -->
    <!-- SplitList callbacks -->
    <xsl:template name="resource">
        <xsl:param name="arg"/>
        <xsl:param name="item"/>

        <xsl:choose>

            <!-- Data Sets -->
            <xsl:when test="$arg='datasets'">
                <resource name="sync_dataset">
                    <data field="code">
                        <xsl:value-of select="$item"/>
                    </data>
                </resource>
            </xsl:when>

        </xsl:choose>
    </xsl:template>

    <!-- END ************************************************************** -->

</xsl:stylesheet>
