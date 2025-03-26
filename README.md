# Welcome to the End-to-End Stock Use Case developed in Microsoft Fabric

This GitHub repo contains all necessary items to create the End-to-End Stock Use Case in Microsoft Fabric. 

## Prerequisites

To make sure you can use this GitHub repo, a few things are required.

1. You need a Fabric Capacity
2. You need to enable some Tenant Settings
    * [Users can create Fabric items](https://learn.microsoft.com/en-us/fabric/admin/fabric-switch)
    * [Users can synchronize workspace items with their Git repositories](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=github%2CAzure%2Ccommit-to-git)
    * [Users can sync workspaces items with GitHub repositories](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=github%2CAzure%2Ccommit-to-git)
    * Optional: If you wish to activate a Fabric Trial, you need to enable [Users can try Microsoft Fabric paid features](https://learn.microsoft.com/en-us/power-bi/fundamentals/service-self-service-signup-purchase-for-power-bi?tabs=free-sign-up)
3. You need a Workspace backed up by a Fabric Capacity with admin rights to connect your GitHub repo
4. You need a GitHub user


### 1. You need a Fabric Capacity
To be able to use this GitHub repo, a Fabric Capacity is required. You can either use a F2+, a Fabric Trial, or Power BI Premium. To activate a trial, you can follow [this link.](https://learn.microsoft.com/en-us/fabric/fundamentals/fabric-trial)

### 2. You need to enablel some Tenant settings
To make sure you can sync your workspace with GitHub, you need to enable following tenat settings:
* [Users can create Fabric items](https://learn.microsoft.com/en-us/fabric/admin/fabric-switch)
* [Users can synchronize workspace items with their Git repositories](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=github%2CAzure%2Ccommit-to-git)
* [Users can sync workspaces items with GitHub repositories](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=github%2CAzure%2Ccommit-to-git)
* Optional: If you wish to activate a Fabric Trial, you need to enable [Users can try Microsoft Fabric paid features](https://learn.microsoft.com/en-us/power-bi/fundamentals/service-self-service-signup-purchase-for-power-bi?tabs=free-sign-up)

### 3. You need a Workspace backed up by a Fabric Capacity with admin rights to connect your GitHub repo
Make sure your Workspace is assigned to a Capacity - Fabric, Premium or Trial.

<img src="./xxImages/01%20License%20Mode%20Fabric%20Capacity.png" width="500">

Further, make sure you have admin rights on the workspace.

### 4. You need a GitHub user
If you don't have a GitHub user already, please follow [this link](https://github.com/signup?source=login) to create one. In the following screen shots you'll see how to create one with a dummy account. 

Sign up using an email, providing a secure password, and a username

<img src="./xxImages/02%20Sign%20up%20to%20GitHub.png" width="500">

Next, verfiy your account with one option.

<img src="./xxImages/03%20Verify%20your%20account.png" width="500">



## Let's get started
Once all prerequisites are met, you can log in to GitHub and access following [GitHub Repo.](https://github.com/KBubalo/Fabric-Event) From there, press the button to create a fork.

<img src="./xxImages/05%20Create%20a%20GitHub%20Fork.png" width="500">

Choose the owner if you have multiple and give a new repo name if you wish. Per default, the upstream repo (Fabric-Event) is used. Once done, hit the **Create fork** button and wait until it's synced. 

<img src="./xxImages/06%20Create%20Fork.png" width="500">

### Create a personal access token in GitHub
As a next step, we need to create a personal access token for our GitHub which will be used afterwards to sync our Workspace with the GitHub repo. To create one, click on the **top right user icon** and select **Settings**

<img src="./xxImages/07%20GitHub%20User%20Settings.png" width="500">

Scroll down and select **Developer settings**. From there, choose **Personal access tokens** and select **Fine-grained tokens**. Lastly, click the green **Generate new token** button.

<img src="./xxImages/08%20Create%20Personal%20Access%20Token%20GitHub.png" width="500">

Next, you need to provide a Token name, e.g. _Fabric Event_, and give access to the repository. Make sure to only give access to the Fabric-Event repo we just forked. 

<img src="./xxImages/09%20Token%20repo%20access.png" width="500">

Once everything configured, press the **Generate token** button at the bottom of the page. A message will pop up summarizing the configuration. Confirm by pressing the **Generate token** button.


<img src="./xxImages/10%20Token%20generation.png" width="500">

Make sure to copy and store the access token securely as you will not be able to see it again once you leave this page. 


<img src="./xxImages/11%20Copy%20Access%20Token.png" width="500">

As a final step, we need to sync now the GitHub repo with our Workspace. Open Microsoft Fabric, navigate to your workspace and press the **Workspace settings** at the top right. Now, select Git integration, choose GitHub and press the **Add account** button.

<img src="./xxImages/12%20Workspace%20Git%20Integration%20Config.png" width="500">

In the pop-up window, give a Display Name and past your GitHub Personal Access Token which we created before. Hit **Add**.

<img src="./xxImages/13%20Add%20GitHub%20account.png" width="500">

After the account has been successfully added, you can select **Connect**. Now, you have to provide your repo URL, select the main branch, and add **Fabric to the Git folder**. Lasty, click **Connect and sync**.

<img src="./xxImages/14%20Configure%20Repo.png" width="500">

Now, all Fabric items from the GitHub repo will be synced into the workspace. This can take a few minutes. You can follow the process at the top right corner.




[asdf](./xxImages/14%20Configure%20Repo.png)













_ThisItalic_

**bold**


`code` testing code
