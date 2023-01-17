/*
 * Copyright (c) 2022, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.persist.nodegenerator;

import io.ballerina.compiler.syntax.tree.AbstractNodeFactory;
import io.ballerina.compiler.syntax.tree.ArrayDimensionNode;
import io.ballerina.compiler.syntax.tree.ArrayTypeDescriptorNode;
import io.ballerina.compiler.syntax.tree.BuiltinSimpleNameReferenceNode;
import io.ballerina.compiler.syntax.tree.IdentifierToken;
import io.ballerina.compiler.syntax.tree.ImportDeclarationNode;
import io.ballerina.compiler.syntax.tree.ImportOrgNameNode;
import io.ballerina.compiler.syntax.tree.ImportPrefixNode;
import io.ballerina.compiler.syntax.tree.MinutiaeList;
import io.ballerina.compiler.syntax.tree.ModuleMemberDeclarationNode;
import io.ballerina.compiler.syntax.tree.ModulePartNode;
import io.ballerina.compiler.syntax.tree.Node;
import io.ballerina.compiler.syntax.tree.NodeFactory;
import io.ballerina.compiler.syntax.tree.NodeList;
import io.ballerina.compiler.syntax.tree.NodeParser;
import io.ballerina.compiler.syntax.tree.QualifiedNameReferenceNode;
import io.ballerina.compiler.syntax.tree.RecordFieldNode;
import io.ballerina.compiler.syntax.tree.RecordFieldWithDefaultValueNode;
import io.ballerina.compiler.syntax.tree.RecordTypeDescriptorNode;
import io.ballerina.compiler.syntax.tree.SeparatedNodeList;
import io.ballerina.compiler.syntax.tree.SimpleNameReferenceNode;
import io.ballerina.compiler.syntax.tree.SyntaxKind;
import io.ballerina.compiler.syntax.tree.SyntaxTree;
import io.ballerina.compiler.syntax.tree.Token;
import io.ballerina.compiler.syntax.tree.TypeDefinitionNode;
import io.ballerina.compiler.syntax.tree.TypeDescriptorNode;
import io.ballerina.persist.BalException;
import io.ballerina.persist.components.Client;
import io.ballerina.persist.components.ClientResource;
import io.ballerina.persist.components.Function;
import io.ballerina.persist.components.IfElse;
import io.ballerina.persist.components.TypeDescriptor;
import io.ballerina.persist.models.Entity;
import io.ballerina.persist.models.EntityField;
import io.ballerina.persist.models.Module;
import io.ballerina.persist.models.Relation;
import io.ballerina.tools.text.TextDocument;
import io.ballerina.tools.text.TextDocuments;
import org.ballerinalang.formatter.core.Formatter;
import org.ballerinalang.formatter.core.FormatterException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static io.ballerina.compiler.syntax.tree.SyntaxKind.QUALIFIED_NAME_REFERENCE;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.ANYDATASTREAM_IS_STREAM_TYPE;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.ANYDATA_KEYWORD;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.ANYDATA_STREAM_NEXT;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.ANYDATA_STREAM_STATEMENT;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.AUTO_GENERATED_COMMENT;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.AUTO_GENERATED_COMMENT_WITH_REASON;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.CAST_ANYDATA_STREAM;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.COLON;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.COMMA_SPACE;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.DB_CLIENT_IS_DB_CLIENT;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.EMPTY_STRING;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.ERR_IS_ERROR;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.IS_SQL_ERROR;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.KEYWORD_BALLERINAX;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.KEYWORD_ERR;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.KEYWORD_ISOLATED;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.KEYWORD_STREAM;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.KEYWORD_VALUE;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.MYSQL_DRIVER;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.NULLABLE_ANYDATA_STREAM_TYPE;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.NULLABLE_ERROR_STATEMENT;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.PERSIST_ERROR;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.PERSIST_MODULE;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.READ_BY_KEY_RETURN;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.RESULT_IS_ERROR;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.RETURN_CASTED_ERROR;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.RETURN_NILL;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.RETURN_PERSIST_ERROR_CLOSE_STREAM;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.RETURN_PERSIST_ERROR_FROM_DBCLIENT;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.SELF_ERR;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.SPACE;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.SPECIFIC_ERROR;
import static io.ballerina.persist.nodegenerator.BalSyntaxConstants.VALUE;
import static io.ballerina.persist.nodegenerator.SyntaxTokenConstants.SYNTAX_TREE_SEMICOLON;

/**
 * Class containing methods to create and read ballerina files as syntax trees.
 *
 * @since 0.1.0
 */
public class BalSyntaxGenerator {

    private BalSyntaxGenerator() {
    }

    /**
     * method to read ballerina files.
     */
    public static void populateEntities(Module.Builder moduleBuilder, SyntaxTree balSyntaxTree) throws IOException,
            BalException {
        ModulePartNode rootNote = balSyntaxTree.rootNode();
        NodeList<ModuleMemberDeclarationNode> nodeList = rootNote.members();
        Entity.Builder entityBuilder = null;
        for (ModuleMemberDeclarationNode moduleNode : nodeList) {
            if (moduleNode.kind() != SyntaxKind.TYPE_DEFINITION) {
                continue;
            }
            TypeDefinitionNode typeDefinitionNode = (TypeDefinitionNode) moduleNode;
            entityBuilder = Entity.newBuilder(typeDefinitionNode.typeName().text().trim());
            entityBuilder.setDeclarationNode(moduleNode);

            List<EntityField> keyArray = new ArrayList<>();
            RecordTypeDescriptorNode recordDesc = (RecordTypeDescriptorNode) ((TypeDefinitionNode) moduleNode)
                    .typeDescriptor();
            for (Node node : recordDesc.fields()) {
                EntityField.Builder fieldBuilder;
                String qualifiedNamePrefix = null;
                if (node.kind() == SyntaxKind.RECORD_FIELD_WITH_DEFAULT_VALUE) {
                    RecordFieldWithDefaultValueNode fieldNode = (RecordFieldWithDefaultValueNode) node;

                    fieldBuilder = EntityField.newBuilder(fieldNode.fieldName().text().trim());
                    String fType;
                    TypeDescriptorNode type;
                    if (fieldNode.typeName().kind().equals(SyntaxKind.ARRAY_TYPE_DESC)) {
                        type = ((ArrayTypeDescriptorNode) fieldNode.typeName()).memberTypeDesc();
                        fieldBuilder.setArrayType(true);
                    } else {
                        type = (TypeDescriptorNode) fieldNode.typeName();
                    }
                    fType = getType(type, fieldNode.fieldName().text().trim());
                    qualifiedNamePrefix = getQualifiedModulePrefix(type);
                    fieldBuilder.setType(fType);
                    EntityField entityField = fieldBuilder.build();
                    entityBuilder.addField(entityField);
                    if (fieldNode.readonlyKeyword().isPresent()) {
                        keyArray.add(entityField);
                    }
                } else if (node.kind() == SyntaxKind.RECORD_FIELD) {
                    RecordFieldNode fieldNode = (RecordFieldNode) node;
                    fieldBuilder = EntityField.newBuilder(fieldNode.fieldName().text().trim());
                    String fType;
                    TypeDescriptorNode type;
                    if (fieldNode.typeName().kind().equals(SyntaxKind.ARRAY_TYPE_DESC)) {
                        type = ((ArrayTypeDescriptorNode) fieldNode.typeName()).memberTypeDesc();
                        fieldBuilder.setArrayType(true);
                    } else {
                        type = (TypeDescriptorNode) fieldNode.typeName();
                    }
                    fType = getType(type, fieldNode.fieldName().text().trim());
                    qualifiedNamePrefix = getQualifiedModulePrefix(type);
                    fieldBuilder.setType(fType);
                    EntityField entityField = fieldBuilder.build();
                    entityBuilder.addField(entityField);
                    if (fieldNode.readonlyKeyword().isPresent()) {
                        keyArray.add(entityField);
                    }
                }
                if (qualifiedNamePrefix != null) {
                    moduleBuilder.addImportModulePrefix(qualifiedNamePrefix);
                }
            }
            entityBuilder.setKeys(keyArray);
            Entity entity = entityBuilder.build();
            moduleBuilder.addEntity(entity.getEntityName(), entity);
        }
    }

    private static String getType(TypeDescriptorNode typeDesc, String fieldName) throws BalException {
        switch (typeDesc.kind()) {
            case INT_TYPE_DESC:
            case BOOLEAN_TYPE_DESC:
            case DECIMAL_TYPE_DESC:
            case FLOAT_TYPE_DESC:
            case STRING_TYPE_DESC:
            case BYTE_TYPE_DESC:
                return ((BuiltinSimpleNameReferenceNode) typeDesc).name().text();
            case QUALIFIED_NAME_REFERENCE:
                QualifiedNameReferenceNode qualifiedName = (QualifiedNameReferenceNode) typeDesc;
                String modulePrefix = qualifiedName.modulePrefix().text();
                String identifier = qualifiedName.identifier().text();
                return modulePrefix + COLON + identifier;
            case SIMPLE_NAME_REFERENCE:
                return ((SimpleNameReferenceNode) typeDesc).name().text();
            default:
                throw new BalException(String.format("Unsupported data type found for the field `%s`", fieldName));
        }
    }

    private static String getQualifiedModulePrefix(TypeDescriptorNode typeDesc) {
        if (typeDesc.kind() == QUALIFIED_NAME_REFERENCE) {
            QualifiedNameReferenceNode qualifiedName = (QualifiedNameReferenceNode) typeDesc;
            return qualifiedName.modulePrefix().text();
        } else {
            return null;
        }
    }

    public static void inferRelationDetails(Module entityModule) {
        Map<String, Entity> entityMap = entityModule.getEntityMap();
        for (Entity entity : entityMap.values()) {
            List<EntityField> fields = entity.getFields();
            fields.stream().filter(field -> entityMap.get(field.getFieldType()) != null)
                    .forEach(field -> {
                        String fieldType = field.getFieldType();
                        Entity assocEntity = entityMap.get(fieldType);
                        if (field.getRelation() == null) {
                            // this branch only handles one-to-many or many-to-many with no relation annotations
                            assocEntity.getFields().stream().filter(assocfield -> assocfield.getFieldType()
                                            .equals(entity.getEntityName()))
                                    .filter(assocfield -> assocfield.getRelation() == null).forEach(assocfield -> {
                                        // one-to-many or many-to-many with no relation annotations
                                        if (field.isArrayType() && assocfield.isArrayType()) {
                                            throw new RuntimeException("Unsupported many to many relation between " +
                                                    entity.getEntityName() + " and " + assocEntity.getEntityName());
                                        }
                                        if (field.isArrayType()) {
                                            // one-to-many relation. associated entity is the owner.
                                            field.setRelation(computeRelation(entity, assocEntity, false));
                                            assocfield.setRelation(computeRelation(assocEntity, entity, true));
                                        } else {
                                            // one-to-many relation. entity is the owner.
                                            // one-to-one relation. entity is the owner.
                                            field.setRelation(computeRelation(entity, assocEntity, true));
                                            assocfield.setRelation(computeRelation(assocEntity, entity, false));
                                        }
                                    });
                        } else if (field.getRelation() != null && field.getRelation().isOwner()) {
                            field.getRelation().setRelationType(field.isArrayType() ?
                                    Relation.RelationType.MANY : Relation.RelationType.ONE);
                            field.getRelation().setAssocEntity(assocEntity);
                            List<Relation.Key> keyColumns = field.getRelation().getKeyColumns();
                            if (keyColumns == null || keyColumns.size() == 0) {
                                keyColumns = assocEntity.getKeys().stream().map(key ->
                                        new Relation.Key(assocEntity.getEntityName().toLowerCase(Locale.ENGLISH)
                                                + key.getFieldName().substring(0, 1).toUpperCase(Locale.ENGLISH)
                                                + key.getFieldName().substring(1),
                                                key.getFieldName(), key.getFieldType())).collect(Collectors.toList());
                                field.getRelation().setKeyColumns(keyColumns);
                            }
                            List<String> references = field.getRelation().getReferences();
                            if (references == null || references.size() == 0) {
                                field.getRelation().setReferences(assocEntity.getKeys().stream()
                                        .map(EntityField::getFieldName)
                                        .collect(Collectors.toList()));
                            }

                            // create bidirectional mapping for associated entity
                            Relation.Builder assocRelBuilder = Relation.newBuilder();
                            assocRelBuilder.setOwner(false);
                            assocRelBuilder.setAssocEntity(entity);

                            List<Relation.Key> assockeyColumns = assocEntity.getKeys().stream().map(key ->
                                    new Relation.Key(key.getFieldName(),
                                            assocEntity.getEntityName().toLowerCase(Locale.ENGLISH)
                                                    + key.getFieldName().substring(0, 1).toUpperCase(Locale.ENGLISH)
                                                    + key.getFieldName().substring(1), key.getFieldType()))
                                    .collect(Collectors.toList());
                            assocRelBuilder.setKeys(assockeyColumns);
                            assocRelBuilder.setReferences(assockeyColumns.stream().map(Relation.Key::getReference)
                                    .collect(Collectors.toList()));
                            assocEntity.getFields().stream().filter(assocfield -> assocfield.getFieldType()
                                    .equals(entity.getEntityName())).forEach(
                                    assocField -> {
                                        assocRelBuilder.setRelationType(assocField.isArrayType() ?
                                                Relation.RelationType.MANY : Relation.RelationType.ONE);
                                        assocField.setRelation(assocRelBuilder.build());
                                    });
                        }
                    });
        }
    }

    private static Relation computeRelation(Entity entity, Entity assocEntity, boolean isOwner) {
        Relation.Builder relBuilder = new Relation.Builder();
        relBuilder.setAssocEntity(assocEntity);
        if (isOwner) {
            List<Relation.Key> keyColumns = assocEntity.getKeys().stream().map(key ->
                    new Relation.Key(assocEntity.getEntityName().toLowerCase(Locale.ENGLISH)
                            + key.getFieldName().substring(0, 1).toUpperCase(Locale.ENGLISH)
                            + key.getFieldName().substring(1), key.getFieldName(), key.getFieldType()
                            )).collect(Collectors.toList());
            relBuilder.setOwner(true);
            relBuilder.setRelationType(Relation.RelationType.ONE);
            relBuilder.setKeys(keyColumns);
            relBuilder.setReferences(assocEntity.getKeys().stream().map(EntityField::getFieldName)
                    .collect(Collectors.toList()));
        } else {
            List<Relation.Key> keyColumns = entity.getKeys().stream().map(key ->
                    new Relation.Key(key.getFieldName(),
                            entity.getEntityName().toLowerCase(Locale.ENGLISH)
                            + key.getFieldName().substring(0, 1).toUpperCase(Locale.ENGLISH)
                            + key.getFieldName().substring(1), key.getFieldType())).collect(Collectors.toList());
            relBuilder.setOwner(false);
            relBuilder.setRelationType(Relation.RelationType.MANY);
            relBuilder.setKeys(keyColumns);
            relBuilder.setReferences(keyColumns.stream().map(Relation.Key::getReference).collect(Collectors.toList()));
        }
        return relBuilder.build();
    }



    public static SyntaxTree generateClientSyntaxTree(Module entityModule) throws BalException {
        //ArrayList<ImportDeclarationNode> importsArray;
        NodeList<ImportDeclarationNode> imports = AbstractNodeFactory.createEmptyNodeList();
        NodeList<ModuleMemberDeclarationNode> moduleMembers = AbstractNodeFactory.createEmptyNodeList();

        MinutiaeList commentMinutiaeList = createCommentMinutiaeList(String.format(AUTO_GENERATED_COMMENT_WITH_REASON,
                entityModule.getModuleName()));
        imports = imports.add(getImportDeclarationNodeWithAutogeneratedComment(BalSyntaxConstants.KEYWORD_BALLERINA,
                BalSyntaxConstants.PERSIST_MODULE, commentMinutiaeList, null));
        // TODO: uncomment the code for the implementation
//        imports = imports.add(getImportDeclarationNode(BalSyntaxConstants.KEYWORD_BALLERINA,
//                PERSIST_MODULE, null));
//        imports = imports.add(getImportDeclarationNode(BalSyntaxConstants.KEYWORD_BALLERINA,
//                KEYWORD_SQL, null));

//        if (clientObject.isTimeImport()) {
//            if (importsArray.isEmpty()) {
//                importsArray.add(getImportDeclarationNode(BalSyntaxConstants.KEYWORD_BALLERINA,
//                        BalSyntaxConstants.KEYWORD_TIME, null));
//            }
//            imports = imports.add(getImportDeclarationNode(BalSyntaxConstants.KEYWORD_BALLERINA,
//                    BalSyntaxConstants.KEYWORD_TIME, null));
//        }
//        Class client = createClientResource(entity, subFields, keys, keyType, keyAutoInc);
//
        Client clientObject = createClient(entityModule);
        moduleMembers = moduleMembers.add(clientObject.getClassDefinitionNode());

        // TODO: uncomment the code for the implementation
//        Class clientStream = createClientStreamClass(entity, entity.getEntityName());
//
//        moduleMembers = moduleMembers.add(clientStream.getClassDefinitionNode());
//

        Token eofToken = AbstractNodeFactory.createIdentifierToken(EMPTY_STRING);
        ModulePartNode modulePartNode = NodeFactory.createModulePartNode(imports, moduleMembers, eofToken);
        TextDocument textDocument = TextDocuments.from(EMPTY_STRING);
        SyntaxTree balTree = SyntaxTree.from(textDocument);
        return balTree.modifyWith(modulePartNode);
    }

    private static Client createClient(Module entityModule) throws BalException {
        Client clientObject = new Client(entityModule.getClientName(), true);
        clientObject.addQualifiers(new String[]{BalSyntaxConstants.KEYWORD_CLIENT});
        clientObject.addMember(NodeFactory.createTypeReferenceNode(
                AbstractNodeFactory.createToken(SyntaxKind.ASTERISK_TOKEN),
                NodeFactory.createQualifiedNameReferenceNode(
                        NodeFactory.createIdentifierToken(
                                BalSyntaxConstants.InheritedTypeReferenceConstants.PERSIST_MODULE_NAME),
                        AbstractNodeFactory.createToken(SyntaxKind.COLON_TOKEN),
                        NodeFactory.createIdentifierToken(
                                BalSyntaxConstants.InheritedTypeReferenceConstants.ABSTRACT_PERSIST_CLIENT)
                ),
                AbstractNodeFactory.createToken(SyntaxKind.SEMICOLON_TOKEN)), false);
        clientObject.addMember(NodeFactory.createBasicLiteralNode(SyntaxKind.STRING_LITERAL,
                AbstractNodeFactory.createLiteralValueToken(SyntaxKind.STRING_LITERAL, SPACE,
                        NodeFactory.createEmptyMinutiaeList(), NodeFactory.createEmptyMinutiaeList())), false);

        Collection<Entity> entityArray = entityModule.getEntityMap().values();
        if (entityArray.size() == 0) {
            throw new BalException("No entities found in the schema file.");
        }
        List<ClientResource> resourceList = new ArrayList<>();
        for (Entity entity : entityArray) {
            resourceList.add(createClientResource(entity));
        }
        // TODO: uncomment the code for the implementation
//        List<Node> fields = new ArrayList<>();
//        resourceList.forEach(resource -> {
//            if (!fields.isEmpty()) {
//                fields.add(NodeFactory.createBasicLiteralNode(SyntaxKind.STRING_LITERAL,
//                        AbstractNodeFactory.createLiteralValueToken(SyntaxKind.STRING_LITERAL, COMMA_SPACE
//                                        + System.lineSeparator(), NodeFactory.createEmptyMinutiaeList(),
//                                NodeFactory.createEmptyMinutiaeList())));
//            }
//            fields.add(NodeFactory.createSpecificFieldNode(null,
//                    AbstractNodeFactory.createIdentifierToken(resource.getResourceName().text()),
//                    SyntaxTokenConstants.SYNTAX_TREE_COLON,
//                    NodeFactory.createMappingConstructorExpressionNode(
//                            SyntaxTokenConstants.SYNTAX_TREE_OPEN_BRACE, AbstractNodeFactory
//                                    .createSeparatedNodeList(resource.getMetadata()),
//                            SyntaxTokenConstants.SYNTAX_TREE_CLOSE_BRACE)));
//        });


//        clientObject.addMember(TypeDescriptor.getObjectFieldNodeWithoutExpression(BalSyntaxConstants.KEYWORD_PRIVATE,
//                        new String[]{BalSyntaxConstants.KEYWORD_FINAL},
//                        TypeDescriptor.getSimpleNameReferenceNode("map<persist:SQLClient>"),
//                        BalSyntaxConstants.PERSIST_CLIENTS),
//                true);
//        clientObject.addMember(TypeDescriptor.getObjectFieldNode(BalSyntaxConstants.KEYWORD_PRIVATE,
//                new String[]{BalSyntaxConstants.KEYWORD_FINAL},
//                TypeDescriptor.getSimpleNameReferenceNode("map<persist:Metadata>"),
//                "metadata", NodeFactory.createMappingConstructorExpressionNode(
//                        SyntaxTokenConstants.SYNTAX_TREE_OPEN_BRACE, AbstractNodeFactory
//                                .createSeparatedNodeList(fields),
//                        SyntaxTokenConstants.SYNTAX_TREE_CLOSE_BRACE)), true);

        // TODO: uncomment the code for the implementation
//        Function init = createInitFunction(entityArray);
//        clientObject.addMember(init.getFunctionDefinitionNode(), true);

        resourceList.forEach(resource -> {
            resource.getFunctions().forEach(function -> {
                clientObject.addMember(function, false);
                    });
                });

        // TODO: uncomment the code for the implementation
//        Function close = createCloseFunction();
//        clientObject.addMember(close.getFunctionDefinitionNode(), true);
        return clientObject;
    }


    private static ClientResource createClientResource(Entity entity) {
        HashMap<String, String> keys = new HashMap<>();
        ClientResource resource = new ClientResource(entity.getTableName());
        // TODO: uncomment the code for the implementation
//        resource.addMetadata(NodeFactory.createSpecificFieldNode(null,
//                AbstractNodeFactory.createIdentifierToken("entityName"),
//                SyntaxTokenConstants.SYNTAX_TREE_COLON,
//                NodeFactory.createBasicLiteralNode(SyntaxKind.STRING_LITERAL,
//                        NodeFactory.createLiteralValueToken(SyntaxKind.STRING_LITERAL_TOKEN,
//                                "\"" + entity.getEntityName().trim() + "\"", NodeFactory.createEmptyMinutiaeList(),
//                                NodeFactory.createEmptyMinutiaeList()))));
//        resource.addMetadata(NodeFactory.createSpecificFieldNode(null,
//                AbstractNodeFactory.createIdentifierToken("tableName"),
//                SyntaxTokenConstants.SYNTAX_TREE_COLON,
//                NodeFactory.createBasicLiteralNode(SyntaxKind.STRING_LITERAL,
//                        NodeFactory.createLiteralValueToken(SyntaxKind.STRING_LITERAL_TOKEN,
//                                "\"" + entity.getTableName().trim() + "\"", NodeFactory.createEmptyMinutiaeList(),
//                                NodeFactory.createEmptyMinutiaeList()))));

        for (EntityField field : entity.getKeys()) {
            keys.put(field.getFieldName(), field.getFieldType());
        }
            // TODO: uncomment the code for the implementation
//            if (!fields.isEmpty()) {
//                fields.add(NodeFactory.createBasicLiteralNode(SyntaxKind.STRING_LITERAL,
//                        AbstractNodeFactory.createLiteralValueToken(SyntaxKind.STRING_LITERAL, COMMA_SPACE
//                                        + System.lineSeparator(), NodeFactory.createEmptyMinutiaeList(),
//                                NodeFactory.createEmptyMinutiaeList())));
//            }
//
//            fields.add(NodeFactory.createSpecificFieldNode(null,
//                    AbstractNodeFactory.createIdentifierToken(field.getFieldName()),
//                    SyntaxTokenConstants.SYNTAX_TREE_COLON,
//                    NodeParser.parseExpression(String.format(BalSyntaxConstants.FIELD_FORMAT_WITHOUT_AUTO_G,
//                            field.getFieldName().trim().replaceAll(
//                                    SINGLE_QUOTE, EMPTY_STRING),
//                            field.getFieldType().trim().replaceAll(SPACE,
//                                    EMPTY_STRING)
//                    ))));

        // TODO: uncomment the code for the implementation
//        resource.addMetadata(NodeFactory.createSpecificFieldNode(null,
//                AbstractNodeFactory.createIdentifierToken(BalSyntaxConstants.TAG_FIELD_METADATA),
//                SyntaxTokenConstants.SYNTAX_TREE_COLON, NodeFactory.createMappingConstructorExpressionNode(
//                        SyntaxTokenConstants.SYNTAX_TREE_OPEN_BRACE, AbstractNodeFactory
//                                .createSeparatedNodeList(fields),
//                        SyntaxTokenConstants.SYNTAX_TREE_CLOSE_BRACE)));
//
//        StringBuilder keysString = new StringBuilder();
//        for (String key : entity.getKeys()) {
//            if (keysString.length() > 0) {
//                keysString.append(COMMA_SPACE);
//            }
//            keysString.append(DOUBLE_QUOTE).append(key).append(DOUBLE_QUOTE);
//        }
//
//        resource.addMetadata(NodeFactory.createSpecificFieldNode(null,
//                AbstractNodeFactory.createIdentifierToken("keyFields"),
//                SyntaxTokenConstants.SYNTAX_TREE_COLON, NodeFactory.createListConstructorExpressionNode(
//                        SyntaxTokenConstants.SYNTAX_TREE_OPEN_BRACKET, AbstractNodeFactory
//                                .createSeparatedNodeList(NodeFactory.createBasicLiteralNode(SyntaxKind.STRING_LITERAL,
//                                        AbstractNodeFactory.createLiteralValueToken(SyntaxKind.STRING_LITERAL,
//                                                keysString.toString(), NodeFactory.createEmptyMinutiaeList(),
//                                                NodeFactory.createEmptyMinutiaeList())))
//                        , SyntaxTokenConstants.SYNTAX_TREE_CLOSE_BRACKET)
//        ));


        Function read = createGetFunction(entity);
        resource.addFunction(read.getFunctionDefinitionNode(), true);

        Function readByKey = createGetByKeyFunction(entity, keys);
        resource.addFunction(readByKey.getFunctionDefinitionNode(), true);

        Function create = createPostFunction(entity, keys);
        resource.addFunction(create.getFunctionDefinitionNode(), true);

        Function update = createPutFunction(entity, keys);
        resource.addFunction(update.getFunctionDefinitionNode(), true);

        Function delete = createDeleteFunction(entity, keys);
        resource.addFunction(delete.getFunctionDefinitionNode(), true);

        return resource;
    }

    private static Client createClientStreamClass(Entity entity, String className) {
        Client clientStream = new Client(className + KEYWORD_STREAM, true);

        clientStream.addMember(NodeFactory.createBasicLiteralNode(SyntaxKind.STRING_LITERAL,
                AbstractNodeFactory.createLiteralValueToken(SyntaxKind.STRING_LITERAL, SPACE,
                        NodeFactory.createEmptyMinutiaeList(), NodeFactory.createEmptyMinutiaeList())), true);

        clientStream.addMember(NodeParser.parseStatement(ANYDATA_STREAM_STATEMENT), false);
        clientStream.addMember(NodeParser.parseStatement(NULLABLE_ERROR_STATEMENT), false);

        Function initStream = new Function(BalSyntaxConstants.INIT, SyntaxKind.OBJECT_METHOD_DEFINITION);
        initStream.addQualifiers(new String[]{BalSyntaxConstants.KEYWORD_PUBLIC, KEYWORD_ISOLATED});
        initStream.addStatement(NodeParser.parseStatement(BalSyntaxConstants.INIT_STREAM_STATEMENT));
        initStream.addStatement(NodeParser.parseStatement(SELF_ERR));
        initStream.addRequiredParameter(NodeParser.parseTypeDescriptor(NULLABLE_ANYDATA_STREAM_TYPE), ANYDATA_KEYWORD);
        initStream.addRequiredParameterWithDefault(TypeDescriptor.getOptionalTypeDescriptorNode(EMPTY_STRING,
                PERSIST_ERROR), KEYWORD_ERR, Function.Bracket.PAREN);
        clientStream.addMember(initStream.getFunctionDefinitionNode(), true);

        Function nextStream = new Function(BalSyntaxConstants.NEXT, SyntaxKind.OBJECT_METHOD_DEFINITION);
        nextStream.addQualifiers(new String[]{BalSyntaxConstants.KEYWORD_PUBLIC, KEYWORD_ISOLATED});
        nextStream.addReturns(NodeParser.parseTypeDescriptor(String.format(
                BalSyntaxConstants.NEXT_STREAM_RETURN_TYPE, entity.getEntityName())));

        IfElse errorCheck = new IfElse(NodeParser.parseExpression(ERR_IS_ERROR));
        errorCheck.addIfStatement(NodeParser.parseStatement(RETURN_CASTED_ERROR));
        IfElse streamCheck = new IfElse(NodeParser.parseExpression(ANYDATASTREAM_IS_STREAM_TYPE));

        streamCheck.addIfStatement(NodeParser.parseStatement(CAST_ANYDATA_STREAM));
        streamCheck.addIfStatement(NodeParser.parseStatement(ANYDATA_STREAM_NEXT));

        IfElse streamValueNilCheck = new IfElse(NodeParser.parseExpression(
                BalSyntaxConstants.NEXT_STREAM_IF_STATEMENT));
        streamValueNilCheck.addIfStatement(NodeParser.parseStatement(
                BalSyntaxConstants.NEXT_STREAM_RETURN_STREAM_VALUE));
        IfElse streamValueErrorCheck = new IfElse(NodeParser.parseExpression(
                BalSyntaxConstants.NEXT_STREAM_ELSE_IF_STATEMENT));
        streamValueErrorCheck.addIfStatement(NodeParser.parseStatement(
                BalSyntaxConstants.NEXT_STREAM_RETURN_STREAM_VALUE_ERROR));
        streamValueErrorCheck.addElseStatement(NodeParser.parseStatement(String.format(
                BalSyntaxConstants.NEXT_STREAM_ELSE_STATEMENT, entity.getEntityName(), entity.getEntityName())));
        streamValueErrorCheck.addElseStatement(NodeParser.parseStatement(BalSyntaxConstants.RETURN_NEXT_RECORD));
        streamValueNilCheck.addElseBody(streamValueErrorCheck);
        streamCheck.addIfStatement(streamValueNilCheck.getIfElseStatementNode());
        streamCheck.addElseStatement(NodeParser.parseStatement(RETURN_NILL));
        errorCheck.addElseBody(streamCheck);
        nextStream.addIfElseStatement(errorCheck.getIfElseStatementNode());
        clientStream.addMember(nextStream.getFunctionDefinitionNode(), true);

        Function closeStream = new Function(BalSyntaxConstants.CLOSE, SyntaxKind.OBJECT_METHOD_DEFINITION);
        closeStream.addQualifiers(new String[]{BalSyntaxConstants.KEYWORD_PUBLIC, KEYWORD_ISOLATED});
        closeStream.addReturns(TypeDescriptor.getOptionalTypeDescriptorNode(EMPTY_STRING,
                PERSIST_ERROR));
        streamCheck = new IfElse(NodeParser.parseExpression(ANYDATASTREAM_IS_STREAM_TYPE));
        streamCheck.addIfStatement(NodeParser.parseStatement(CAST_ANYDATA_STREAM));
        streamCheck.addIfStatement(NodeParser.parseStatement(BalSyntaxConstants.CLOSE_STREAM_STATEMENT));
        IfElse sqlErrorCheck = new IfElse(NodeParser.parseExpression(IS_SQL_ERROR));
        sqlErrorCheck.addIfStatement(NodeParser.parseStatement(RETURN_PERSIST_ERROR_CLOSE_STREAM));
        streamCheck.addIfStatement(sqlErrorCheck.getIfElseStatementNode());
        closeStream.addIfElseStatement(streamCheck.getIfElseStatementNode());
        clientStream.addMember(closeStream.getFunctionDefinitionNode(), true);
        return clientStream;
    }

    private static Function createInitFunction(Collection<Entity> entityArray) {
        Function init = new Function(BalSyntaxConstants.INIT, SyntaxKind.OBJECT_METHOD_DEFINITION);
        init.addQualifiers(new String[]{BalSyntaxConstants.KEYWORD_PUBLIC});
        init.addReturns(TypeDescriptor.getOptionalTypeDescriptorNode(EMPTY_STRING,
                PERSIST_ERROR));
        init.addStatement(NodeParser.parseStatement(BalSyntaxConstants.INIT_MYSQL_CLIENT));
        IfElse errorCheck = new IfElse(NodeParser.parseExpression(DB_CLIENT_IS_DB_CLIENT));
        errorCheck.addIfStatement(NodeParser.parseStatement(RETURN_PERSIST_ERROR_FROM_DBCLIENT));
        init.addIfElseStatement(errorCheck.getIfElseStatementNode());

        List<Node> clients = new ArrayList<>();
        entityArray.forEach(entity -> {
            if (!clients.isEmpty()) {
                clients.add(NodeFactory.createBasicLiteralNode(SyntaxKind.STRING_LITERAL,
                        AbstractNodeFactory.createLiteralValueToken(SyntaxKind.STRING_LITERAL, COMMA_SPACE
                                        + System.lineSeparator(), NodeFactory.createEmptyMinutiaeList(),
                                NodeFactory.createEmptyMinutiaeList())));
            }
            clients.add(NodeFactory.createSpecificFieldNode(null,
                    AbstractNodeFactory.createIdentifierToken(entity.getTableName()),
                    SyntaxTokenConstants.SYNTAX_TREE_COLON,
                    NodeParser.parseExpression(String.format(BalSyntaxConstants.INIT_PERSIST_CLIENT,
                            entity.getTableName(), entity.getTableName(),
                            entity.getTableName(), entity.getTableName()))));
        });
        init.addStatement(NodeFactory.createAssignmentStatementNode(
                NodeFactory.createFieldAccessExpressionNode(NodeFactory.createSimpleNameReferenceNode(
                        NodeFactory.createIdentifierToken("self")),
                        AbstractNodeFactory.createToken(SyntaxKind.DOT_TOKEN),
                        NodeFactory.createSimpleNameReferenceNode(
                                NodeFactory.createIdentifierToken("persistClients"))),
                SyntaxTokenConstants.SYNTAX_TREE_EQUAL, NodeFactory.createMappingConstructorExpressionNode(
                        SyntaxTokenConstants.SYNTAX_TREE_OPEN_BRACE, AbstractNodeFactory
                                .createSeparatedNodeList(clients),
                        SyntaxTokenConstants.SYNTAX_TREE_CLOSE_BRACE), SYNTAX_TREE_SEMICOLON));
        return init;
    }

    private static Function createPostFunction(Entity entity,
                                               HashMap<String, String> keys) {
        Function create = new Function(BalSyntaxConstants.POST, SyntaxKind.RESOURCE_ACCESSOR_DEFINITION);
        NodeList<Node> resourcePaths = AbstractNodeFactory.createEmptyNodeList();
        resourcePaths = resourcePaths.add(AbstractNodeFactory.createIdentifierToken(entity.getTableName()));
        create.addRelativeResourcePaths(resourcePaths);
        create.addRequiredParameter(
                TypeDescriptor.getArrayTypeDescriptorNode(
                        String.format("%sInsert", entity.getEntityName())), KEYWORD_VALUE);
        create.addQualifiers(new String[]{KEYWORD_ISOLATED, BalSyntaxConstants.KEYWORD_RESOURCE});
        if (keys.size() == 1) {
            create.addReturns(TypeDescriptor.getUnionTypeDescriptorNode(
                    TypeDescriptor.getArrayTypeDescriptorNode(keys.values().stream().findFirst().get()),
                    TypeDescriptor.getQualifiedNameReferenceNode(PERSIST_MODULE, SPECIFIC_ERROR)));
        } else {
            List<Node> typeList = new ArrayList<>();
            keys.values().forEach(value -> {
                if (!typeList.isEmpty()) {
                    typeList.add(NodeFactory.createToken(SyntaxKind.COMMA_TOKEN));
                }
                typeList.add(NodeFactory.createSimpleNameReferenceNode(NodeFactory.createIdentifierToken(value)));
            });
            ArrayDimensionNode arrayDimensionNode = NodeFactory.createArrayDimensionNode(
                    SyntaxTokenConstants.SYNTAX_TREE_OPEN_BRACKET,
                    null,
                    SyntaxTokenConstants.SYNTAX_TREE_CLOSE_BRACKET
            );
            NodeList<ArrayDimensionNode> dimensionList = NodeFactory.createNodeList(arrayDimensionNode);
            create.addReturns(TypeDescriptor.getUnionTypeDescriptorNode(
                    NodeFactory.createArrayTypeDescriptorNode(NodeFactory.createTupleTypeDescriptorNode(
                            NodeFactory.createToken(SyntaxKind.OPEN_BRACKET_TOKEN),
                            NodeFactory.createSeparatedNodeList(typeList),
                            NodeFactory.createToken(SyntaxKind.CLOSE_BRACKET_TOKEN)
                    ), dimensionList), TypeDescriptor.getQualifiedNameReferenceNode(PERSIST_MODULE, SPECIFIC_ERROR)));
        }
        // TODO: uncomment the code for the implementation
//        create.addStatement(NodeParser.parseStatement(String.format(BalSyntaxConstants.CREATE_SQL_RESULTS,
//                entity.getTableName())));
//        if (keys.keySet().size() == 1) {
//            create.addStatement(NodeParser.parseStatement(
//                    String.format(BalSyntaxConstants.CREATE_SQL_RESULTS_SINGLE_KEY,
//                            entity.getEntityName() + "Insert",
//                            "inserted." + keys.keySet().stream().findFirst().get())));
//        } else {
//            StringBuilder filterKeys = new StringBuilder();
//            for (String entry : keys.keySet()) {
//                if (filterKeys.length() != 0) {
//                    filterKeys.append(',');
//                }
//                filterKeys.append("inserted.").append(entry);
//            }
//            create.addStatement(NodeParser.parseStatement(
//                    String.format(BalSyntaxConstants.CREATE_SQL_RESULTS_SINGLE_KEY,
//                            entity.getEntityName() + "Insert", filterKeys)));
//        }
        return create;
    }

    private static Function createGetByKeyFunction(Entity entity, HashMap<String, String> keys) {
        Function readByKey = new Function(BalSyntaxConstants.GET, SyntaxKind.RESOURCE_ACCESSOR_DEFINITION);
        NodeList<Node> resourcePaths = AbstractNodeFactory.createEmptyNodeList();
        resourcePaths = resourcePaths.add(AbstractNodeFactory.createIdentifierToken(entity.getTableName()));

        for (Map.Entry<String, String> entry : keys.entrySet()) {
            resourcePaths = resourcePaths.add(AbstractNodeFactory.createToken(SyntaxKind.SLASH_TOKEN));
            resourcePaths = resourcePaths.add(NodeFactory.createResourcePathParameterNode(
                    SyntaxKind.RESOURCE_PATH_SEGMENT_PARAM,
                    AbstractNodeFactory.createToken(SyntaxKind.OPEN_BRACKET_TOKEN),
                    AbstractNodeFactory.createEmptyNodeList(),
                    NodeFactory.createBuiltinSimpleNameReferenceNode(SyntaxKind.STRING_TYPE_DESC,
                            AbstractNodeFactory.createIdentifierToken(entry.getValue() + " ")),
                    null,
                    AbstractNodeFactory.createIdentifierToken(entry.getKey()),
                    AbstractNodeFactory.createToken(SyntaxKind.CLOSE_BRACKET_TOKEN)));

        }
        readByKey.addRelativeResourcePaths(resourcePaths);
        readByKey.addQualifiers(new String[]{KEYWORD_ISOLATED, BalSyntaxConstants.KEYWORD_RESOURCE});
        readByKey.addReturns(TypeDescriptor.getUnionTypeDescriptorNode(
                TypeDescriptor.getSimpleNameReferenceNode(entity.getEntityName()),
                TypeDescriptor.getQualifiedNameReferenceNode(PERSIST_MODULE, SPECIFIC_ERROR)));

        //TODO: Handle composite keys with creating record value
        if (keys.size() == 1) {
            readByKey.addStatement(NodeParser.parseStatement(String.format(READ_BY_KEY_RETURN,
                    entity.getTableName(), entity.getEntityName(), keys.keySet().stream().findFirst().get(),
                    entity.getEntityName())));
        }
        return readByKey;
    }

    private static Function createGetFunction(Entity entity) {
        Function read = new Function(BalSyntaxConstants.GET, SyntaxKind.RESOURCE_ACCESSOR_DEFINITION);
        read.addQualifiers(new String[]{KEYWORD_ISOLATED, BalSyntaxConstants.KEYWORD_RESOURCE});
        NodeList<Node> resourcePaths = AbstractNodeFactory.createEmptyNodeList();
        resourcePaths = resourcePaths.add(AbstractNodeFactory.createIdentifierToken(entity.getTableName()));
        read.addRelativeResourcePaths(resourcePaths);
        read.addReturns(TypeDescriptor.getStreamTypeDescriptorNode(
                NodeFactory.createSimpleNameReferenceNode(AbstractNodeFactory.createIdentifierToken(
                        entity.getEntityName())),
                NodeFactory.createOptionalTypeDescriptorNode(
                        NodeFactory.createQualifiedNameReferenceNode(
                                AbstractNodeFactory.createIdentifierToken("persist"),
                                AbstractNodeFactory.createToken(SyntaxKind.COLON_TOKEN),
                                AbstractNodeFactory.createIdentifierToken("Error")),
                                AbstractNodeFactory.createToken(SyntaxKind.QUESTION_MARK_TOKEN)
                )));
        read.addStatement(NodeParser.parseStatement(String.format(BalSyntaxConstants.READ_RUN_READ_QUERY,
                entity.getTableName(), entity.getEntityName())));
        IfElse errorCheck = new IfElse(NodeParser.parseExpression(RESULT_IS_ERROR));
        errorCheck.addIfStatement(NodeParser.parseStatement(String.format(
                BalSyntaxConstants.READ_RETURN_STREAM_WHEN_ERROR, entity.getEntityName(), entity.getEntityName())));
        errorCheck.addElseStatement(NodeParser.parseStatement(String.format(
                BalSyntaxConstants.READ_RETURN_STREAM_WHEN_NOT_ERROR, entity.getEntityName(), entity.getEntityName())));

        read.addIfElseStatement(errorCheck.getIfElseStatementNode());
        return read;
    }

    private static Function createPutFunction(Entity entity, HashMap<String, String> keys) {
        Function update = new Function(BalSyntaxConstants.PUT, SyntaxKind.RESOURCE_ACCESSOR_DEFINITION);
        update.addQualifiers(new String[]{KEYWORD_ISOLATED, BalSyntaxConstants.KEYWORD_RESOURCE});
        NodeList<Node> resourcePaths = AbstractNodeFactory.createEmptyNodeList();
        resourcePaths = resourcePaths.add(AbstractNodeFactory.createIdentifierToken(entity.getTableName()));

        for (Map.Entry<String, String> entry : keys.entrySet()) {
            resourcePaths = resourcePaths.add(AbstractNodeFactory.createToken(SyntaxKind.SLASH_TOKEN));
            resourcePaths = resourcePaths.add(NodeFactory.createResourcePathParameterNode(
                    SyntaxKind.RESOURCE_PATH_SEGMENT_PARAM,
                    AbstractNodeFactory.createToken(SyntaxKind.OPEN_BRACKET_TOKEN),
                    AbstractNodeFactory.createEmptyNodeList(),
                    NodeFactory.createBuiltinSimpleNameReferenceNode(SyntaxKind.STRING_TYPE_DESC,
                            AbstractNodeFactory.createIdentifierToken(entry.getValue() + " ")),
                    null,
                    AbstractNodeFactory.createIdentifierToken(entry.getKey()),
                    AbstractNodeFactory.createToken(SyntaxKind.CLOSE_BRACKET_TOKEN)));

        }
        update.addRelativeResourcePaths(resourcePaths);

        update.addReturns(TypeDescriptor.getUnionTypeDescriptorNode(
                TypeDescriptor.getSimpleNameReferenceNode(entity.getEntityName()),
                TypeDescriptor.getQualifiedNameReferenceNode(PERSIST_MODULE, SPECIFIC_ERROR)));
        update.addStatement(NodeParser.parseStatement(BalSyntaxConstants.UPDATE_RUN_UPDATE_QUERY));
        update.addRequiredParameter(TypeDescriptor.getSimpleNameReferenceNode(
                String.format("%sUpdate", entity.getEntityName())), VALUE);
        return update;
    }

    private static Function createDeleteFunction(Entity entity, HashMap<String, String> keys) {
        Function delete = new Function(BalSyntaxConstants.DELETE, SyntaxKind.RESOURCE_ACCESSOR_DEFINITION);
        //delete.addRequiredParameter(TypeDescriptor.getSimpleNameReferenceNode(entity.getEntityName()), KEYWORD_VALUE);
        delete.addQualifiers(new String[]{KEYWORD_ISOLATED, BalSyntaxConstants.KEYWORD_RESOURCE});
        NodeList<Node> resourcePaths = AbstractNodeFactory.createEmptyNodeList();
        resourcePaths = resourcePaths.add(AbstractNodeFactory.createIdentifierToken(entity.getTableName()));

        for (Map.Entry<String, String> entry : keys.entrySet()) {
            resourcePaths = resourcePaths.add(AbstractNodeFactory.createToken(SyntaxKind.SLASH_TOKEN));
            resourcePaths = resourcePaths.add(NodeFactory.createResourcePathParameterNode(
                    SyntaxKind.RESOURCE_PATH_SEGMENT_PARAM,
                    AbstractNodeFactory.createToken(SyntaxKind.OPEN_BRACKET_TOKEN),
                    AbstractNodeFactory.createEmptyNodeList(),
                    NodeFactory.createBuiltinSimpleNameReferenceNode(SyntaxKind.STRING_TYPE_DESC,
                            AbstractNodeFactory.createIdentifierToken(entry.getValue() + " ")),
                    null,
                    AbstractNodeFactory.createIdentifierToken(entry.getKey()),
                    AbstractNodeFactory.createToken(SyntaxKind.CLOSE_BRACKET_TOKEN)));

        }
        delete.addRelativeResourcePaths(resourcePaths);
        delete.addReturns(TypeDescriptor.getUnionTypeDescriptorNode(
                TypeDescriptor.getSimpleNameReferenceNode(entity.getEntityName()),
                TypeDescriptor.getQualifiedNameReferenceNode(PERSIST_MODULE, SPECIFIC_ERROR)));
        delete.addStatement(NodeParser.parseStatement(BalSyntaxConstants.DELETE_RUN_DELETE_QUERY));
        return delete;
    }

    private static Function createCloseFunction() {
        Function close = new Function(BalSyntaxConstants.CLOSE, SyntaxKind.OBJECT_METHOD_DEFINITION);
        close.addQualifiers(new String[]{BalSyntaxConstants.KEYWORD_PUBLIC});
        close.addReturns(TypeDescriptor.getOptionalTypeDescriptorNode(EMPTY_STRING,
                PERSIST_ERROR));
        close.addStatement(NodeParser.parseStatement(BalSyntaxConstants.CLOSE_PERSIST_CLIENT));
        return close;
    }

    public static String generateDatabaseConfigSyntaxTree() throws FormatterException {
        NodeList<ImportDeclarationNode> imports = AbstractNodeFactory.createEmptyNodeList();
        NodeList<ModuleMemberDeclarationNode> moduleMembers = AbstractNodeFactory.createEmptyNodeList();

        MinutiaeList commentMinutiaeList = createCommentMinutiaeList(String.format(AUTO_GENERATED_COMMENT));
        ImportPrefixNode prefix = NodeFactory.createImportPrefixNode(SyntaxTokenConstants.SYNTAX_TREE_AS,
                AbstractNodeFactory.createToken(SyntaxKind.UNDERSCORE_KEYWORD));
        imports = imports.add(getImportDeclarationNodeWithAutogeneratedComment(KEYWORD_BALLERINAX, MYSQL_DRIVER,
                commentMinutiaeList, prefix));
        moduleMembers = moduleMembers.add(NodeParser.parseModuleMemberDeclaration(
                BalSyntaxConstants.CONFIGURABLE_PORT));
        moduleMembers = moduleMembers.add(NodeParser.parseModuleMemberDeclaration(
                BalSyntaxConstants.CONFIGURABLE_HOST));
        moduleMembers = moduleMembers.add(NodeParser.parseModuleMemberDeclaration(
                BalSyntaxConstants.CONFIGURABLE_USER));
        moduleMembers = moduleMembers.add(NodeParser.parseModuleMemberDeclaration(
                BalSyntaxConstants.CONFIGURABLE_DATABASE));
        moduleMembers = moduleMembers.add(NodeParser.parseModuleMemberDeclaration(
                BalSyntaxConstants.CONFIGURABLE_PASSWORD));

        Token eofToken = AbstractNodeFactory.createIdentifierToken(EMPTY_STRING);
        ModulePartNode modulePartNode = NodeFactory.createModulePartNode(imports, moduleMembers, eofToken);
        TextDocument textDocument = TextDocuments.from(EMPTY_STRING);
        SyntaxTree balTree = SyntaxTree.from(textDocument);

        // output cannot be SyntaxTree as it will overlap with Toml Syntax Tree in Init Command
        return Formatter.format(balTree.modifyWith(modulePartNode).toSourceCode());
    }

    public static String generateSchemaSyntaxTree() throws FormatterException {
        NodeList<ImportDeclarationNode> imports = AbstractNodeFactory.createEmptyNodeList();
        NodeList<ModuleMemberDeclarationNode> moduleMembers = AbstractNodeFactory.createEmptyNodeList();

        imports = imports.add(NodeParser.parseImportDeclaration("import ballerina/persist as _;"));
        Token eofToken = AbstractNodeFactory.createIdentifierToken(EMPTY_STRING);
        ModulePartNode modulePartNode = NodeFactory.createModulePartNode(imports, moduleMembers, eofToken);
        TextDocument textDocument = TextDocuments.from(EMPTY_STRING);
        SyntaxTree balTree = SyntaxTree.from(textDocument);

        // output cannot be SyntaxTree as it will overlap with Toml Syntax Tree in Init Command
        return Formatter.format(balTree.modifyWith(modulePartNode).toSourceCode());
    }

    private static ImportDeclarationNode getImportDeclarationNode(String orgName, String moduleName,
                                                                 ImportPrefixNode prefix) {
        Token orgNameToken = AbstractNodeFactory.createIdentifierToken(orgName);
        ImportOrgNameNode importOrgNameNode = NodeFactory.createImportOrgNameNode(
                orgNameToken,
                SyntaxTokenConstants.SYNTAX_TREE_SLASH
        );
        Token moduleNameToken = AbstractNodeFactory.createIdentifierToken(moduleName);
        SeparatedNodeList<IdentifierToken> moduleNodeList =
                AbstractNodeFactory.createSeparatedNodeList(moduleNameToken);

        return NodeFactory.createImportDeclarationNode(
                SyntaxTokenConstants.SYNTAX_TREE_KEYWORD_IMPORT,
                importOrgNameNode,
                moduleNodeList,
                prefix,
                SYNTAX_TREE_SEMICOLON
        );
    }

    private static MinutiaeList createCommentMinutiaeList(String comment) {
        return  NodeFactory.createMinutiaeList(
                AbstractNodeFactory.createCommentMinutiae(BalSyntaxConstants.AUTOGENERATED_FILE_COMMENT),
                AbstractNodeFactory.createEndOfLineMinutiae(System.lineSeparator()),
                AbstractNodeFactory.createEndOfLineMinutiae(System.lineSeparator()),
                AbstractNodeFactory.createCommentMinutiae(comment),
                AbstractNodeFactory.createEndOfLineMinutiae(System.lineSeparator()),
                AbstractNodeFactory.createCommentMinutiae(BalSyntaxConstants.COMMENT_SHOULD_NOT_BE_MODIFIED),
                AbstractNodeFactory.createEndOfLineMinutiae(System.lineSeparator()),
                AbstractNodeFactory.createEndOfLineMinutiae(System.lineSeparator()));
    }

    private static ImportDeclarationNode getImportDeclarationNodeWithAutogeneratedComment(
            String orgName, String moduleName, MinutiaeList commentMinutiaeList, ImportPrefixNode prefix) {
        Token orgNameToken = AbstractNodeFactory.createIdentifierToken(orgName);
        ImportOrgNameNode importOrgNameNode = NodeFactory.createImportOrgNameNode(
                orgNameToken,
                SyntaxTokenConstants.SYNTAX_TREE_SLASH
        );
        Token moduleNameToken = AbstractNodeFactory.createIdentifierToken(moduleName);
        SeparatedNodeList<IdentifierToken> moduleNodeList =
                AbstractNodeFactory.createSeparatedNodeList(moduleNameToken);
        Token importToken = NodeFactory.createToken(SyntaxKind.IMPORT_KEYWORD,
                commentMinutiaeList, NodeFactory.createMinutiaeList(AbstractNodeFactory
                        .createWhitespaceMinutiae(SPACE)));
        return NodeFactory.createImportDeclarationNode(
                importToken,
                importOrgNameNode,
                moduleNodeList,
                prefix,
                SYNTAX_TREE_SEMICOLON
        );
    }

    public static SyntaxTree generateTypeSyntaxTree(Module entityModule) {
        NodeList<ImportDeclarationNode> imports = AbstractNodeFactory.createEmptyNodeList();
        NodeList<ModuleMemberDeclarationNode> moduleMembers = AbstractNodeFactory.createEmptyNodeList();
        MinutiaeList commentMinutiaeList = createCommentMinutiaeList(String.format(
                AUTO_GENERATED_COMMENT_WITH_REASON, entityModule.getModuleName()));
        boolean timeImport = false;

        for (String modulePrefix : entityModule.getImportModulePrefixes()) {
            if (imports.isEmpty()) {
                imports = imports.add(getImportDeclarationNodeWithAutogeneratedComment(
                        BalSyntaxConstants.KEYWORD_BALLERINA, modulePrefix,
                        commentMinutiaeList, null));
            } else {
                imports.add(getImportDeclarationNode(BalSyntaxConstants.KEYWORD_BALLERINA, modulePrefix, null));
            }
        }
        for (Entity entity : entityModule.getEntityMap().values()) {
            moduleMembers = moduleMembers.add(createEntityRecord(entity));

            moduleMembers = moduleMembers.add(NodeParser.parseModuleMemberDeclaration(
                    String.format("public type %sInsert %s;", entity.getEntityName(),
                            entity.getEntityName())));
            moduleMembers = moduleMembers.add(createUpdateRecord(entity));

            // TODO: improve import logic
//            if (entity.getFields().stream().anyMatch(entityField ->
//                    entityField.getFieldType().trim().startsWith(BalSyntaxConstants.KEYWORD_TIME_PREFIX))) {
//                timeImport = true;
//            }
        }
        Token eofToken = AbstractNodeFactory.createIdentifierToken(EMPTY_STRING);
        ModulePartNode modulePartNode = NodeFactory.createModulePartNode(imports, moduleMembers, eofToken);
        TextDocument textDocument = TextDocuments.from(EMPTY_STRING);
        SyntaxTree balTree = SyntaxTree.from(textDocument);

        return balTree.modifyWith(modulePartNode);
    }

    private static ModuleMemberDeclarationNode createEntityRecord(Entity entity) {
        StringBuilder recordFields = new StringBuilder();
        for (EntityField field : entity.getFields()) {
            if (entity.getKeys().stream().anyMatch(key -> key == field)) {
                recordFields.append("readonly ");
                recordFields.append(" ");
                recordFields.append(field.getFieldType());
                recordFields.append(" ");
                recordFields.append(field.getFieldName());
                recordFields.append("; ");
            } else if (field.getRelation() != null) {
                if (field.getRelation().isOwner()) {
                    for (Relation.Key key : field.getRelation().getKeyColumns()) {
                        recordFields.append(key.getType());
                        recordFields.append(" ");
                        recordFields.append(key.getField());
                        recordFields.append("; ");
                    }
                }
            } else {
                recordFields.append(field.getFieldType());
                recordFields.append(" ");
                recordFields.append(field.getFieldName());
                recordFields.append("; ");
            }

        }
        return NodeParser.parseModuleMemberDeclaration(String.format("public type %s record {| %s |};",
                entity.getEntityName().trim(), recordFields));
    }

    private static ModuleMemberDeclarationNode createUpdateRecord(Entity entity) {
        StringBuilder recordFields = new StringBuilder();
        for (EntityField field : entity.getFields()) {
            if (entity.getKeys().stream().noneMatch(key -> key == field)) {
                if (field.getRelation() != null) {
                    if (field.getRelation().isOwner()) {
                        for (Relation.Key key : field.getRelation().getKeyColumns()) {
                            recordFields.append(key.getType());
                            recordFields.append(" ");
                            recordFields.append(key.getField());
                            recordFields.append("?; ");
                        }
                    }
                } else {
                    recordFields.append(field.getFieldType());
                    recordFields.append(" ");
                    recordFields.append(field.getFieldName());
                    recordFields.append("?; ");
                }
            }

        }
        return NodeParser.parseModuleMemberDeclaration(String.format("public type %sUpdate record {| %s |};",
                entity.getEntityName().trim(), recordFields));
    }
}
