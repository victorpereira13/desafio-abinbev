# Project Setup Guide

## AWS EC2 Setup

1. **Log in to the AWS Management Console**:
   - Navigate to AWS Management Console.

2. **Navigate to the EC2 service**:
   - In the AWS Management Console, select "EC2" from the services menu.

3. **Launch a new EC2 instance**:
   - Click “Launch Instance” and follow the prompts to create a new EC2 instance.

4. **Configure security groups**:
   - Ensure necessary inbound and outbound traffic is allowed.

5. **SSH into the instance**:
   - Use your preferred SSH client to connect to the instance.

## Azure Blob Storage Setup

### Create a Storage Account

1. **Sign in to the Azure portal**:
   - Go to Azure Portal.

2. **Navigate to Storage Accounts**:
   - In the left-hand menu, select “Storage accounts”.

3. **Create a new storage account**:
   - Click on “Create”.
   - Fill in the required details:
     - **Subscription**: Select your subscription.
     - **Resource group**: Create a new resource group or select an existing one.
     - **Storage account name**: Enter a unique name for your storage account.
     - **Region**: Choose the region closest to your users.
     - **Performance**: Select “Standard” for most use cases.
     - **Replication**: Choose the replication option that suits your needs (e.g., LRS, GRS).
   - Click “Review + create” and then “Create”.

### Create a Blob Container

1. **Navigate to your storage account**:
   - Once the storage account is created, go to the storage account overview.

2. **Create a container**:
   - Click on “Containers” under the “Data storage” section.
   - Click on “+ Container”.
   - Enter a name for your container (e.g., “bronze”, “silver”, “gold”).
   - Set the public access level (e.g., “Private” for secure access).
   - Click “Create”.

### Configure Access and Permissions

1. **Access keys**:
   - In the storage account overview, click on “Access keys” under the “Security + networking” section.
   - Copy the connection string or access keys.

This step is crucial as it enables Databricks to communicate with Azure Blob Storage.
